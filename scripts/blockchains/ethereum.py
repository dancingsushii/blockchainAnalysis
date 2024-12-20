import sqlite3
from typing import List

import pandas as pd
from collections import Counter
from datetime import datetime
import json
from scripts.common.plotting import PlottingUtils
from scripts.common.utils import PathManager


class EthereumAnalyzer:
    def __init__(self):
        self.paths = PathManager.get_paths()
        self.data = None
        self.processed_data = {
            'geographic': None,
            'client': None,
            'hosting': None
        }
        self.DB_PATH = "/Users/tetianayakovenko/Desktop/masterThesis/GitHubRepository/blockchainAnalysis/data/processed/blockchains/ethereum/ethereum_crawler.db"
        self.client_mappings = {
            r'(?i)^geth.*': 'Geth',
            r'(?i)^go-ethereum.*': 'Geth',
            r'(?i)^erigon.*': 'Erigon',
            r'(?i)^nethermind.*': 'Nethermind',
            r'(?i)^besu.*': 'Besu',
            # Consensus Layer
            r'(?i)^prysm.*': 'Prysm',
            r'(?i)^lighthouse.*': 'Lighthouse',
            r'(?i)^teku.*': 'Teku',
            r'(?i)^nimbus.*': 'Nimbus',
            r'(?i)^lodestar.*': 'Lodestar'
        }
        self.raw_data_path = PathManager.ROOT_DIR / "data" / "raw" / "ethereum" / "nebula_ethereum_nodes.json"

    def fetch_data_from_json(self):
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

    def _is_valid_ip(self, ip: str) -> bool:
        """Basic validation for IPv4 addresses."""
        parts = ip.split('.')
        if len(parts) != 4:
            return False
        try:
            return all(0 <= int(part) <= 255 for part in parts)
        except ValueError:
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
        return list(set(ips))  # Remove duplicates

    def fetch_data(self):
        """Load data from the nodes table in SQLite database."""
        try:
            conn = sqlite3.connect(self.DB_PATH)
            query = "SELECT * FROM nodes"
            self.data = pd.read_sql(query, conn)
            conn.close()

            non_ethereum_clients = ['bor', 'coregeth']
            self.data = self.data[~self.data["name"].isin(non_ethereum_clients)]
            self.data = self.data[~self.data["name"].str.contains(r"tmp|placeholder|invalid", case=False, na=False)]

            print(f"Successfully fetched data with {len(self.data)} nodes")
            return True
        except Exception as e:
            print(f"Error fetching data: {e}")
            return False

    def process_geographic_distribution(self):
        """Process node data to extract country distribution."""
        if self.data is None:
            return pd.DataFrame(columns=['Category', 'Count'])

        country_counts = Counter(self.data['country_name'].dropna())
        return pd.DataFrame(
            list(country_counts.items()),
            columns=['Category', 'Count']
        ).sort_values(by='Count', ascending=False)

    def process_client_distribution(self):
        """Process node data to extract client distribution for valid Ethereum clients."""
        if self.data is None:
            return pd.DataFrame(columns=['Category', 'Count'])

        normalized_names = self.data['name'].copy()
        for pattern, replacement in self.client_mappings.items():
            normalized_names = normalized_names.str.replace(pattern, replacement, regex=True)

        valid_clients = [
            'Geth',
            'Erigon',
            'Nethermind',
            'Besu',

            'Prysm',
            'Lighthouse',
            'Teku',
            'Nimbus',
            'Lodestar'
        ]

        valid_client_data = normalized_names[normalized_names.isin(valid_clients)]
        client_counts = Counter(valid_client_data.dropna())

        return pd.DataFrame(
            list(client_counts.items()),
            columns=['Category', 'Count']
        ).sort_values(by='Count', ascending=False)

    def process_hosting_distribution(self):
        """Process node data to extract hosting provider distribution."""
        import geoip2.database

        if not self.raw_data_path.exists():
            print(f"Error: Raw data file not found at {self.raw_data_path}")
            return pd.DataFrame(columns=['Category', 'Count'])

        hosting_counts = Counter()

        try:
            with geoip2.database.Reader('data/GeoLite2-ASN.mmdb') as reader:
                with open(self.raw_data_path, 'r') as file:
                    for line in file:
                        if line.strip():
                            try:
                                node_data = json.loads(line)
                                maddrs = node_data.get('Maddrs', [])
                                if maddrs is None:
                                    continue

                                ips = self._extract_ips(maddrs)
                                if ips:
                                    for ip in ips:
                                        try:
                                            response = reader.asn(ip)
                                            org = response.autonomous_system_organization
                                            if org:
                                                # Clean up provider names
                                                org = (org.replace(' LLC', '')
                                                       .replace(' Ltd.', '')
                                                       .replace(' Inc.', '')
                                                       .replace(' Corporation', '')
                                                       .replace(' Corp.', '')
                                                       .strip())
                                                hosting_counts[org] += 1
                                        except Exception as e:
                                            continue
                            except json.JSONDecodeError as e:
                                continue

        except Exception as e:
            print(f"Error processing hosting distribution: {e}")
            import traceback
            print(traceback.format_exc())

        df = pd.DataFrame(
            list(hosting_counts.items()),
            columns=['Category', 'Count']
        ).sort_values(by='Count', ascending=False)

        return df

    def process_data(self):
        """Process all distributions and save to CSV."""
        if self.data is None:
            print("No data to process")
            return False

        self.processed_data['geographic'] = self.process_geographic_distribution()
        self.processed_data['geographic'].to_csv(self.paths['data']['geographic'], index=False)
        print(f"\nGeographic distribution saved to {self.paths['data']['geographic']}")
        print("\nTop 10 countries by node count:")
        print(self.processed_data['geographic'].head(10))

        self.processed_data['client'] = self.process_client_distribution()
        self.processed_data['client'].to_csv(self.paths['data']['client'], index=False)
        print(f"\nClient distribution saved to {self.paths['data']['client']}")
        print("\nTop 10 clients by node count:")
        print(self.processed_data['client'].head(10))

        self.processed_data['hosting'] = self.process_hosting_distribution()
        self.processed_data['hosting'].to_csv(self.paths['data']['hosting'], index=False)
        print(f"\nHosting distribution saved to {self.paths['data']['hosting']}")
        print("\nTop 10 IP ranges by node count:")
        print(self.processed_data['hosting'].head(10))

        return True

    def create_visualizations(self):
        """Create all visualizations."""
        try:
            PlottingUtils.plot_pie_chart_with_filtered_legend(
                self.paths['data']['geographic'],
                "Ethereum geographic distribution",
                self.paths['plots']['geographic'],
                convert_countries=True
            )

            PlottingUtils.plot_pie_chart_with_filtered_legend(
                self.paths['data']['client'],
                "Ethereum client distribution",
                self.paths['plots']['client'],
                convert_countries=False
            )

            PlottingUtils.plot_pie_chart_with_filtered_legend(
                self.paths['data']['hosting'],
                "Ethereum hosting distribution",
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

    parser = argparse.ArgumentParser(description='Ethereum Network Analysis')
    parser.add_argument('--process-only', action='store_true', help='Only process data')
    parser.add_argument('--plot-only', action='store_true', help='Only create plots')
    args = parser.parse_args()

    analyzer = EthereumAnalyzer()

    if args.process_only:
        analyzer.fetch_data()
        analyzer.process_data()
    elif args.plot_only:
        analyzer.create_visualizations()
    else:
        analyzer.run_analysis()