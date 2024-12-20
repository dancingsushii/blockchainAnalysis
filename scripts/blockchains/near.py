import json
from datetime import datetime
import pandas as pd
from collections import Counter
from scripts.common.utils import PathManager, APIUtils, CountryMapper
import geoip2.database


class NEARAnalyzer:
    def __init__(self):
        self.paths = PathManager.get_paths()
        self.data = None
        self.processed_data = {
            'geographic': None,
            'hosting': None
        }
        self.ENDPOINT = 'https://rpc.mainnet.near.org'
        self.RPC_PAYLOAD = {
            "jsonrpc": "2.0",
            "id": "",
            "method": "network_info",
            "params": []
        }

    def fetch_data(self):
        """Fetch NEAR node data from RPC API."""
        response = APIUtils.make_request(
            url=self.ENDPOINT,
            method='POST',
            json=self.RPC_PAYLOAD
        )

        if response and 'result' in response and 'active_peers' in response['result']:
            self.data = response['result']
            print(f"Successfully fetched data with {len(self.data['active_peers'])} peers")
            return True
        print("Failed to fetch data or invalid data format")
        return False

    def process_geographic_distribution(self):
        """Process node data to extract country distribution."""
        country_counts = Counter()

        try:
            if 'active_peers' in self.data:
                with geoip2.database.Reader('data/GeoLite2-Country.mmdb') as reader:
                    for peer in self.data['active_peers']:
                        try:
                            ip = peer.get('addr', '').split(':')[0]
                            ip = ip.replace('[', '').replace(']', '')  # Remove IPv6 brackets

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

        if 'active_peers' in self.data:
            print(self.data['active_peers'])
            with geoip2.database.Reader('data/GeoLite2-ASN.mmdb') as reader:
                for peer in self.data['active_peers']:
                    try:
                        ip = peer.get('addr', '').split(':')[0]
                        ip = ip.replace('[', '').replace(']', '')

                        response = reader.asn(ip)
                        if response.autonomous_system_organization:
                            hosting_counts[response.autonomous_system_organization] += 1
                        else:
                            hosting_counts['Unknown'] += 1
                    except Exception as e:
                        hosting_counts['Unknown'] += 1
                        continue

        return pd.DataFrame(
            list(hosting_counts.items()),
            columns=['Category', 'Count']
        ).sort_values(by='Count', ascending=False)

    def process_data(self):
        """Process all distributions and save to CSV."""
        if not self.data:
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
            "nodes": self.data
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
                "NEAR geographic distribution",
                self.paths['plots']['geographic'],
                convert_countries=True
            )

            PlottingUtils.plot_pie_chart_with_filtered_legend(
                self.paths['data']['hosting'],
                "NEAR hosting distribution",
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

    parser = argparse.ArgumentParser(description='NEAR Network Analysis')
    parser.add_argument('--process-only', action='store_true', help='Only process data')
    parser.add_argument('--plot-only', action='store_true', help='Only create plots')
    args = parser.parse_args()

    analyzer = NEARAnalyzer()

    if args.process_only:
        analyzer.fetch_data()
        analyzer.process_data()
    elif args.plot_only:
        analyzer.create_visualizations()
    else:
        analyzer.run_analysis()