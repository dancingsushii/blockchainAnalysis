import json
import socket
from datetime import datetime
import pandas as pd
from collections import Counter
from scripts.common.utils import PathManager, APIUtils
import geoip2.database


class StellarAnalyzer:
    def __init__(self):
        self.paths = PathManager.get_paths()
        self.data = None
        self.processed_data = {
            'geographic': None,
            'hosting': None
        }
        self.ENDPOINT = 'https://api.stellarbeat.io/v1/node'

    def fetch_data(self):
        """Fetch Stellar node data from API."""
        response = APIUtils.make_request(self.ENDPOINT)
        if response:
            self.data = response
            print(f"Successfully fetched data with {len(self.data)} nodes")
            return True
        print("Failed to fetch data")

        print(f"Sample node data: {self.data[0] if self.data else 'No data'}")
        return False

    def process_geographic_distribution(self):
        """Process node data to extract country distribution."""
        country_counts = Counter()

        for node in self.data:
            try:
                if (node.get('geoData') and
                        node.get('active') and
                        'countryCode' in node['geoData']):
                    country = node['geoData']['countryCode']
                    if country:
                        country_counts[country] += 1
            except Exception as e:
                continue

        return pd.DataFrame(
            list(country_counts.items()),
            columns=['Category', 'Count']
        ).sort_values(by='Count', ascending=False)

    def process_hosting_distribution(self):
        """Process node data to extract hosting provider distribution."""
        hosting_counts = Counter()
        known_providers = {
            'OVH': ['OVH', 'OVH SAS', 'OVH Hosting'],
            'Hetzner': ['Hetzner', 'Hetzner Online', 'Hetzner Online GmbH', 'HETZNER'],
            'DigitalOcean': ['DIGITALOCEAN', 'DigitalOcean', 'DIGITALOCEAN-ASN', 'DIGITALOCEAN-N'],
            'Amazon AWS': ['Amazon', 'Amazon.com', 'AWS', 'AMAZON-AES', 'AMAZON-02', 'AMAZON-', 'Amazon Technologies',
                           'AMAZON'],
            'Google Cloud': ['Google', 'Google Cloud', 'GOOGLE', 'GCP', 'Google LLC'],
            'Microsoft Azure': ['Microsoft', 'Azure', 'Microsoft Corporation', 'MICROSOFT-CORP', 'MICROSOFT'],
            'Vultr': ['Vultr', 'Vultr Holdings', 'VULTR-AS', 'AS-VULTR'],
            'Linode': ['Linode', 'LINODE-AP', 'LINODE', 'Linode LLC'],
            'Equinix': ['Equinix', 'EQUINIX', 'PACKET', 'Packet Host'],
            'Alibaba': ['Alibaba', 'ALIBABA', 'Aliyun']
        }

        try:
            with geoip2.database.Reader('data/GeoLite2-ASN.mmdb') as reader:
                for node in self.data:
                    try:
                        if not node.get('active'):
                            continue
                        if node.get('ip'):
                            ip = node['ip']
                        elif node.get('geoData', {}).get('ip'):
                            ip = node['geoData']['ip']
                        else:
                            continue

                        try:
                            socket.inet_aton(ip)
                        except socket.error:
                            continue

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

                            if not provider_matched and not any(x in org.lower() for x in
                                                                ['residential', 'university', 'college', 'school',
                                                                 'home', 'telecom']):
                                hosting_counts[org] += 1

                    except Exception as e:
                        continue

        except Exception as e:
            print(f"Error processing hosting distribution: {e}")
            return pd.DataFrame(columns=['Category', 'Count'])

        if hosting_counts:
            df = pd.DataFrame(
                list(hosting_counts.items()),
                columns=['Category', 'Count']
            ).sort_values(by='Count', ascending=False)

            total = df['Count'].sum()
            df['Percentage'] = (df['Count'] / total * 100).round(2)

            df = df[df['Count'] > 2]

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
                "Stellar geographic distribution",
                self.paths['plots']['geographic'],
                convert_countries=True
            )

            PlottingUtils.plot_pie_chart_with_filtered_legend(
                self.paths['data']['hosting'],
                "Stellar hosting distribution",
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

    parser = argparse.ArgumentParser(description='Stellar Network Analysis')
    parser.add_argument('--process-only', action='store_true', help='Only process data')
    parser.add_argument('--plot-only', action='store_true', help='Only create plots')
    args = parser.parse_args()

    analyzer = StellarAnalyzer()

    if args.process_only:
        analyzer.fetch_data()
        analyzer.process_data()
    elif args.plot_only:
        analyzer.create_visualizations()
    else:
        analyzer.run_analysis()