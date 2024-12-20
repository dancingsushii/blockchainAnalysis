import pandas as pd
from collections import Counter
from scripts.common.utils import PathManager, APIUtils
import geoip2.database


class DogecoinAnalyzer:
    def __init__(self):
        self.paths = PathManager.get_paths()
        self.data = None
        self.processed_data = {
            'geographic': None,
            'hosting': None
        }
        self.ENDPOINT = 'https://api.blockchair.com/dogecoin/nodes'

    def fetch_data(self):
        """Fetch Dogecoin node data from API."""
        self.data = APIUtils.make_request(self.ENDPOINT)
        if self.data and 'data' in self.data and 'nodes' in self.data['data']:
            nodes = self.data['data']['nodes']
            print(f"Successfully fetched data with {len(nodes)} nodes")
            return True
        return False


    def process_geographic_distribution(self):
        """Process node data to extract country distribution."""
        nodes = self.data['data']['nodes']
        country_counts = Counter()

        for node_info in nodes.values():
            if 'country' in node_info and node_info['country']:
                if node_info.get('version', '').startswith('/Shibetoshi:'):
                    if node_info.get('height', 0) > 0:
                        country = node_info['country']
                        country_counts[country] += 1

        return pd.DataFrame(
            list(country_counts.items()),
            columns=['Category', 'Count']
        ).sort_values(by='Count', ascending=False)

    def process_hosting_distribution(self):
        """Process node data to extract hosting distribution based on ASN information."""
        if not self.data or 'data' not in self.data or 'nodes' not in self.data['data']:
            print("No node data available")
            return pd.DataFrame(columns=['Category', 'Count'])

        hosting_counts = Counter()
        nodes = self.data['data']['nodes']

        known_providers = {
            'OVH': ['OVH', 'OVH SAS', 'OVH Hosting'],
            'Hetzner': ['Hetzner', 'Hetzner Online', 'Hetzner Online GmbH', 'HETZNER'],
            'DigitalOcean': ['DIGITALOCEAN', 'DigitalOcean', 'DIGITALOCEAN-ASN', 'DIGITALOCEAN-N'],
            'Amazon': ['Amazon', 'Amazon.com', 'AWS', 'Amazon AWS', 'AMAZON-AES', 'AMAZON-02'],
            'Google': ['Google', 'Google Cloud', 'GOOGLE', 'GCP', 'Google LLC'],
            'Microsoft Azure': ['Microsoft', 'Azure', 'Microsoft Corporation', 'MICROSOFT-CORP'],
            'Linode': ['Linode', 'LINODE-AP', 'LINODE', 'Linode LLC'],
            'Netcup': ['netcup', 'netcup GmbH'],
            'Vultr': ['Vultr', 'Vultr Holdings', 'VULTR-AS'],
            'Contabo': ['Contabo', 'Contabo GmbH', 'CONTABO']
        }

        try:
            with geoip2.database.Reader('data/GeoLite2-ASN.mmdb') as reader:
                for ip_port, node_info in nodes.items():
                    try:
                        if not node_info.get('version', '').startswith('/Shibetoshi:'):
                            continue

                        if not node_info.get('height', 0) > 0:
                            continue
                        ip = ip_port.split(':')[0]

                        response = reader.asn(ip)
                        org = response.autonomous_system_organization

                        if org:
                            org = (org.replace(' LLC', '')
                                   .replace(' Ltd.', '')
                                   .replace(' Inc.', '')
                                   .replace(' Corporation', '')
                                   .replace(' Corp.', '')
                                   .replace(' SA', '')
                                   .replace(' AG', '')
                                   .strip())

                            provider_matched = False
                            for provider, variations in known_providers.items():
                                if any(variation.lower() in org.lower() for variation in variations):
                                    hosting_counts[provider] += 1
                                    provider_matched = True
                                    break

                            if not provider_matched and not any(x in org.lower() for x in ['relay', 'pool', 'stake']):
                                hosting_counts[org] += 1

                    except Exception as e:
                        continue

        except Exception as e:
            print(f"Error opening ASN database: {e}")
            return pd.DataFrame(columns=['Category', 'Count'])

        if hosting_counts:
            df = pd.DataFrame(
                list(hosting_counts.items()),
                columns=['Category', 'Count']
            ).sort_values(by='Count', ascending=False)

            total = df['Count'].sum()
            df['Percentage'] = (df['Count'] / total * 100).round(2)

            df = df[df['Count'] > 1]

            return df
        else:
            print("Warning: No hosting data could be collected")
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
                "Dogecoin geographic distribution",
                self.paths['plots']['geographic'],
                convert_countries=True
            )

            PlottingUtils.plot_pie_chart_with_filtered_legend(
                self.paths['data']['hosting'],
                "Dogecoin hosting distribution",
                self.paths['plots']['hosting'],
                convert_countries=False
            )

        except Exception as e:
            print(f"Error creating visualizations: {e}")


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
        print(f"\nClient distribution saved to {self.paths['data']['hosting']}")
        print("\nTop 10 hostings by node count:")
        print(self.processed_data['hosting'].head(10))

        return True

    def create_visualizations(self):
        """Create all visualizations."""
        from scripts.common.plotting import PlottingUtils

        try:
            PlottingUtils.plot_pie_chart_with_filtered_legend(
                self.paths['data']['geographic'],
                "Dogecoin geographic distribution",
                self.paths['plots']['geographic'],
                convert_countries=True
            )

            PlottingUtils.plot_pie_chart_with_filtered_legend(
                self.paths['data']['hosting'],
                "Dogecoin hosting distribution",
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

    parser = argparse.ArgumentParser(description='Dogecoin Network Analysis')
    parser.add_argument('--process-only', action='store_true', help='Only process data')
    parser.add_argument('--plot-only', action='store_true', help='Only create plots')
    args = parser.parse_args()

    analyzer = DogecoinAnalyzer()

    if args.process_only:
        analyzer.fetch_data()
        analyzer.process_data()
    elif args.plot_only:
        analyzer.create_visualizations()
    else:
        analyzer.run_analysis()