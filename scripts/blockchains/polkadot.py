import pandas as pd
from collections import Counter
from scripts.common.utils import PathManager
import json
import geoip2.database
from typing import List


class PolkadotAnalyzer:
    def __init__(self):
        self.paths = PathManager.get_paths()
        # Add raw data path
        self.raw_data_path = PathManager.ROOT_DIR / "data" / "raw" / "polkadot" / "nebula_polkadot_nodes.json"
        self.data = None
        self.processed_data = {
            'geographic': None,
            'hosting': None
        }

    def fetch_data(self):
        """Fetch Polkadot data from JSON file."""
        try:
            print("Reading JSON data...")
            print(f"Reading from: {self.raw_data_path}")

            nodes = []
            with open(self.raw_data_path, 'r') as file:
                for line_num, line in enumerate(file, 1):
                    if line.strip():
                        try:
                            node_data = json.loads(line)
                            maddrs = node_data.get('Maddrs')
                            if maddrs is None:
                                continue

                            ips = self._extract_ips(maddrs)
                            if ips:
                                for ip in ips:
                                    node = {
                                        'peer_id': node_data.get('PeerID'),
                                        'ip': ip,
                                        'agent_version': node_data.get('AgentVersion'),
                                        'protocols': node_data.get('Protocols', [])
                                    }
                                    nodes.append(node)
                        except json.JSONDecodeError as e:
                            print(f"Error decoding JSON at line {line_num}: {e}")
                            continue

            self.data = {'nodes': nodes}
            print(f"Successfully processed {len(nodes)} nodes")
            return True
        except Exception as e:
            print(f"Error fetching data: {e}")
            import traceback
            print(traceback.format_exc())
            return False

    def _extract_ips(self, maddrs: List[str]) -> List[str]:
        """Extract IP addresses from multiaddress strings."""
        ips = []
        if not isinstance(maddrs, list):
            return ips

        for addr in maddrs:
            if not isinstance(addr, str):
                continue
            parts = addr.split('/')
            for i, part in enumerate(parts):
                if part == 'ip4' and i + 1 < len(parts):
                    ip = parts[i + 1]
                    if self._is_valid_ip(ip):
                        ips.append(ip)
        return list(set(ips))

    def _is_valid_ip(self, ip: str) -> bool:
        """Basic validation for IPv4 addresses."""
        parts = ip.split('.')
        if len(parts) != 4:
            return False
        try:
            return all(0 <= int(part) <= 255 for part in parts)
        except ValueError:
            return False

    def process_geographic_distribution(self):
        """Process node data to extract country distribution."""
        if not self.data or 'nodes' not in self.data:
            return pd.DataFrame(columns=['Category', 'Count'])

        country_counts = Counter()

        try:
            with geoip2.database.Reader('data/GeoLite2-Country.mmdb') as reader:
                for node in self.data['nodes']:
                    try:
                        if node.get('ip'):
                            response = reader.country(node['ip'])
                            country = response.country.iso_code
                            if country:
                                country_counts[country] += 1
                    except Exception as e:
                        continue

        except Exception as e:
            print(f"Error processing geographic distribution: {e}")

        df = pd.DataFrame(
            list(country_counts.items()),
            columns=['Category', 'Count']
        ).sort_values(by='Count', ascending=False)

        return df

    def process_hosting_distribution(self):
        """Process node data to extract hosting provider distribution."""
        if not self.data or 'nodes' not in self.data:
            return pd.DataFrame(columns=['Category', 'Count'])

        hosting_counts = Counter()

        try:
            with geoip2.database.Reader('data/GeoLite2-ASN.mmdb') as reader:
                for node in self.data['nodes']:
                    try:
                        if node.get('ip'):
                            response = reader.asn(node['ip'])
                            org = response.autonomous_system_organization
                            if org:
                                org = (org.replace(' LLC', '')
                                     .replace(' Ltd.', '')
                                     .replace(' Inc.', '')
                                     .replace(' Corporation', '')
                                     .replace(' Corp.', '')
                                     .strip())
                                hosting_counts[org] += 1
                    except Exception as e:
                        continue

        except Exception as e:
            print(f"Error processing hosting distribution: {e}")

        df = pd.DataFrame(
            list(hosting_counts.items()),
            columns=['Category', 'Count']
        ).sort_values(by='Count', ascending=False)

        return df


    def process_data(self):
        """Process all distributions and save to CSV."""
        if not self.data or 'nodes' not in self.data:
            print("No data to process")
            return False

        self.processed_data['geographic'] = self.process_geographic_distribution()
        self.processed_data['geographic'].to_csv(self.paths['data']['geographic'], index=False)
        print("\nTop 10 countries by node count:")
        print(self.processed_data['geographic'].head(10))

        self.processed_data['hosting'] = self.process_hosting_distribution()
        self.processed_data['hosting'].to_csv(self.paths['data']['hosting'], index=False)
        print("\nTop 10 hosting providers by node count:")
        print(self.processed_data['hosting'].head(10))

        return True

    def create_visualizations(self):
        """Create visualizations for the distributions."""
        from scripts.common.plotting import PlottingUtils

        try:
            PlottingUtils.plot_pie_chart_with_filtered_legend(
                self.paths['data']['geographic'],
                "Polkadot geographic distribution",
                self.paths['plots']['geographic'],
                convert_countries=True
            )

            PlottingUtils.plot_pie_chart_with_filtered_legend(
                self.paths['data']['hosting'],
                "Polkadot hosting distribution",
                self.paths['plots']['hosting'],
                convert_countries=False
            )
        except Exception as e:
            print(f"Error creating visualizations: {e}")

    def run_analysis(self):
        """Run the complete analysis pipeline."""
        if self.fetch_data():
            if self.process_data():
                self.create_visualizations()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Polkadot Network Analysis')
    parser.add_argument('--process-only', action='store_true', help='Only process data')
    parser.add_argument('--plot-only', action='store_true', help='Only create plots')
    args = parser.parse_args()

    analyzer = PolkadotAnalyzer()

    if args.process_only:
        analyzer.fetch_data()
        analyzer.process_data()
    elif args.plot_only:
        analyzer.create_visualizations()
    else:
        analyzer.run_analysis()