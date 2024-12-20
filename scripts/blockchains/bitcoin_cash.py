import pandas as pd
from collections import Counter
from scripts.common.utils import PathManager, APIUtils
import geoip2.database


class BitcoinCashAnalyzer:
    def __init__(self):
        self.paths = PathManager.get_paths()
        self.data = None
        self.processed_data = {
            'geographic': None,
            'client': None,
            'hosting': None
        }
        self.ENDPOINT = 'https://api.blockchair.com/bitcoin-cash/nodes'

    def fetch_data(self):
        """Fetch Bitcoin Cash node data from API."""
        self.data = APIUtils.make_request(self.ENDPOINT)
        if self.data and 'data' in self.data and 'nodes' in self.data['data']:
            nodes = self.data['data']['nodes']
            print(f"Successfully fetched data with {len(nodes)} nodes")
            return True
        return False

    @staticmethod
    def extract_client_name(version_string):
        """Extract standardized client name from version string."""
        try:
            version = version_string.strip('/')
            client = version.split(':')[0]
            return client
        except:
            return 'Unknown'

    def process_geographic_distribution(self):
        """Process node data to extract country distribution."""
        nodes = self.data['data']['nodes']
        country_counts = Counter()

        for node_info in nodes.values():
            try:
                if 'country' in node_info and node_info['country']:
                    if node_info.get('height', 0) > 0:
                        country_counts[node_info['country']] += 1
            except Exception as e:
                continue

        return pd.DataFrame(
            list(country_counts.items()),
            columns=['Category', 'Count']
        ).sort_values(by='Count', ascending=False)

    def process_client_distribution(self):
        """Process node data to extract client distribution."""
        nodes = self.data['data']['nodes']
        client_counts = Counter()

        for node_info in nodes.values():
            try:
                if 'version' in node_info and node_info['version']:
                    if node_info.get('height', 0) > 0:
                        client = self.extract_client_name(node_info['version'])
                        client_counts[client] += 1
            except Exception as e:
                continue

        return pd.DataFrame(
            list(client_counts.items()),
            columns=['Category', 'Count']
        ).sort_values(by='Count', ascending=False)

    def process_hosting_distribution(self):
        """Process node data to extract hosting distribution based on ASN information."""
        if not self.data or 'data' not in self.data or 'nodes' not in self.data['data']:
            print("No node data available")
            return pd.DataFrame(columns=['Category', 'Count'])

        asn_counts = Counter()
        nodes = self.data['data']['nodes']

        try:
            with geoip2.database.Reader('data/GeoLite2-ASN.mmdb') as reader:
                print(f"\nDebug: Processing {len(nodes)} nodes")

                for ip, node_info in nodes.items():
                    try:
                        # Skip if no height or height is 0 (inactive node)
                        if not node_info.get('height', 0):
                            continue

                        # Try IPv4
                        if 'ip' in node_info:
                            ip = node_info['ip']
                        elif isinstance(ip, str):  # Use the dictionary key if it's the IP
                            ip = ip.split(':')[0]  # Remove port if present
                        else:
                            continue

                        # Clean up IP address
                        ip = ip.replace('[', '').replace(']', '')  # Remove IPv6 brackets if present

                        # Get ASN information
                        response = reader.asn(ip)
                        organization = response.autonomous_system_organization

                        if organization:
                            organization = (organization.replace(' LLC', '')
                                            .replace(' Ltd.', '')
                                            .replace(' Inc.', '')
                                            .replace(' Corporation', '')
                                            .replace(' SA', '')
                                            .replace(' AG', '')
                                            .strip())
                            asn_counts[organization] += 1

                    except (ValueError, geoip2.errors.AddressNotFoundError):
                        continue
                    except Exception as e:
                        print(f"Error processing IP {ip}: {str(e)}")
                        continue

        except Exception as e:
            print(f"Error opening ASN database: {e}")
            return pd.DataFrame(columns=['Category', 'Count'])

        print(f"\nDebug: Found {len(asn_counts)} unique hosting providers")

        if asn_counts:
            df = pd.DataFrame(
                list(asn_counts.items()),
                columns=['Category', 'Count']
            ).sort_values(by='Count', ascending=False)

            total = df['Count'].sum()
            df['Percentage'] = (df['Count'] / total * 100).round(2)

            df = df[df['Count'] > 1]

            return df
        else:
            print("Warning: No ASN data could be collected")
            return pd.DataFrame(columns=['Category', 'Count', 'Percentage'])

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

        self.processed_data['client'] = self.process_client_distribution()
        self.processed_data['client'].to_csv(self.paths['data']['client'], index=False)
        print(f"\nClient distribution saved to {self.paths['data']['client']}")
        print("\nTop 10 clients by node count:")
        print(self.processed_data['client'].head(10))

        self.processed_data['hosting'] = self.process_hosting_distribution()
        self.processed_data['hosting'].to_csv(self.paths['data']['hosting'], index=False)
        print(f"\nHosting distribution saved to {self.paths['data']['hosting']}")
        print("\nTop 10 hosting providers by node count:")
        print(self.processed_data['hosting'].head(10))

        return True

    def create_visualizations(self):
        """Create all visualizations."""
        from scripts.common.plotting import PlottingUtils

        try:
            PlottingUtils.plot_pie_chart_with_filtered_legend(
                self.paths['data']['geographic'],
                "Bitcoin Cash geographic distribution",
                self.paths['plots']['geographic'],
                convert_countries=True
            )

            PlottingUtils.plot_pie_chart_with_filtered_legend(
                self.paths['data']['client'],
                "Bitcoin Cash client distribution",
                self.paths['plots']['client'],
                convert_countries=False
            )

            PlottingUtils.plot_pie_chart_with_filtered_legend(
                self.paths['data']['hosting'],
                "Bitcoin Cash hosting distribution",
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

    parser = argparse.ArgumentParser(description='Bitcoin Cash Network Analysis')
    parser.add_argument('--process-only', action='store_true', help='Only process data')
    parser.add_argument('--plot-only', action='store_true', help='Only create plots')
    args = parser.parse_args()

    analyzer = BitcoinCashAnalyzer()

    if args.process_only:
        analyzer.fetch_data()
        analyzer.process_data()
    elif args.plot_only:
        analyzer.create_visualizations()
    else:
        analyzer.run_analysis()
