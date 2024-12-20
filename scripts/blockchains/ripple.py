import json
from datetime import datetime
import pandas as pd
from collections import Counter
from scripts.common.utils import PathManager, APIUtils
import geoip2.database


class RippleAnalyzer:
    def __init__(self):
        self.paths = PathManager.get_paths()
        self.data = None
        self.peer_data = None
        self.processed_data = {
            'geographic': None,
            'hosting': None
        }
        self.ENDPOINT = "https://go.getblock.io/e97ec62471c54056a22778385f50ee0e"
        self.headers = {
            'Content-Type': 'application/json',
            'x-api-key': 'e97ec62471c54056a22778385f50ee0e'
        }

    def fetch_data(self):
        """Fetch Ripple node and peer data from API."""
        try:
            server_info = APIUtils.make_request(
                url=self.ENDPOINT,
                method='POST',
                headers=self.headers,
                json={
                    "jsonrpc": "2.0",
                    "method": "server_info",
                    "params": [],
                    "id": "xrp-e97ec"
                }
            )

            peer_info = APIUtils.make_request(
                url=self.ENDPOINT,
                method='POST',
                headers=self.headers,
                json={
                    "jsonrpc": "2.0",
                    "method": "peers",
                    "params": [],
                    "id": "xrp-e97ec"
                }
            )

            if peer_info and 'result' in peer_info and 'peers' in peer_info['result']:
                self.data = server_info
                self.peer_data = peer_info['result']['peers']
                print(f"Successfully fetched data with {len(self.peer_data)} peers")
                return True
            return False

        except Exception as e:
            print(f"Error fetching data: {e}")
            return False

    def process_geographic_distribution(self):
        """Process node data to extract country distribution."""
        country_counts = Counter()

        try:
            with geoip2.database.Reader('data/GeoLite2-Country.mmdb') as reader:
                for peer in self.peer_data:
                    try:
                        address = peer.get('address', '')
                        ip = address.split(':')[0]
                        ip = ip.replace('[::ffff:', '').replace(']', '')

                        response = reader.country(ip)
                        country = response.country.iso_code
                        if country:
                            country_counts[country] += 1
                    except Exception as e:
                        continue

        except Exception as e:
            print(f"Error processing geographic distribution: {e}")

        return pd.DataFrame(
            list(country_counts.items()),
            columns=['Category', 'Count']
        ).sort_values(by='Count', ascending=False)


    def process_hosting_distribution(self):
        """Process node data to extract hosting provider distribution."""
        hosting_counts = Counter()

        try:
            with geoip2.database.Reader('data/GeoLite2-ASN.mmdb') as reader:
                for peer in self.peer_data:
                    try:
                        address = peer.get('address', '')
                        ip = address.split(':')[0]
                        ip = ip.replace('[::ffff:', '').replace(']', '')

                        response = reader.asn(ip)
                        if response.autonomous_system_organization:
                            hosting_counts[response.autonomous_system_organization] += 1
                        else:
                            hosting_counts['Unknown'] += 1
                    except Exception as e:
                        hosting_counts['Unknown'] += 1
                        continue

        except Exception as e:
            print(f"Error processing hosting distribution: {e}")

        return pd.DataFrame(
            list(hosting_counts.items()),
            columns=['Category', 'Count']
        ).sort_values(by='Count', ascending=False)

    def process_data(self):
        """Process all distributions and save to CSV."""
        if not self.peer_data:
            print("No data to process")
            return False

        self.processed_data['geographic'] = self.process_geographic_distribution()
        self.processed_data['geographic'].to_csv(self.paths['data']['geographic'], index=False)
        print(f"\nGeographic distribution saved to {self.paths['data']['geographic']}")
        print("\nTop 10 countries by node count:")
        print(self.processed_data['geographic'].head(10))


        self.processed_data['hosting'] = self.process_hosting_distribution()
        self.processed_data['hosting'].to_csv(self.paths['data']['hosting'], index=False)
        print(f"\nHosting distribution saved to {self.paths['data']['hosting']}")
        print("\nTop 10 hosting providers by node count:")
        print(self.processed_data['hosting'].head(10))

        node_data = {
            "timestamp": datetime.now().strftime("%m-%d-%Y_%H:%M"),
            "collection_method": "api",
            "chain_data": "",
            "nodes": {
                "server_info": self.data,
                "peers": self.peer_data
            }
        }

        if 'data' in self.paths and 'raw' in self.paths['data']:
            with open(self.paths['data']['raw'], 'w') as f:
                json.dump(node_data, f, indent=4)
            print(f"\nRaw node data saved to {self.paths['data']['raw']}")

        return True

    def create_visualizations(self):
        """Create all visualizations."""
        from scripts.common.plotting import PlottingUtils

        try:
            PlottingUtils.plot_pie_chart_with_filtered_legend(
                self.paths['data']['geographic'],
                "Ripple geographic distribution",
                self.paths['plots']['geographic'],
                convert_countries=True
            )

            PlottingUtils.plot_pie_chart_with_filtered_legend(
                self.paths['data']['hosting'],
                "Ripple hosting distribution",
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

    parser = argparse.ArgumentParser(description='Ripple Network Analysis')
    parser.add_argument('--process-only', action='store_true', help='Only process data')
    parser.add_argument('--plot-only', action='store_true', help='Only create plots')
    args = parser.parse_args()

    analyzer = RippleAnalyzer()

    if args.process_only:
        analyzer.fetch_data()
        analyzer.process_data()
    elif args.plot_only:
        analyzer.create_visualizations()
    else:
        analyzer.run_analysis()