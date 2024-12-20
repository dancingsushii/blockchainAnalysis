from datetime import datetime

import pandas as pd
from collections import Counter
from scripts.common.utils import PathManager, APIUtils
import time
import geoip2.database


class CardanoAnalyzer:
    def __init__(self):
        self.paths = PathManager.get_paths()
        self.data = None
        self.processed_data = {
            'geographic': None,
            'client': None,
            'hosting': None
        }
        self.API_KEY = "mainnetR26eS04QwL3UxF81KSzq4Gj4VsnNn3lO"
        self.BASE_URL = "https://cardano-mainnet.blockfrost.io/api/v0"
        self.headers = {
            "project_id": self.API_KEY
        }

    def fetch_data(self):
        """Fetch Cardano pool and relay data."""
        try:
            pools = APIUtils.make_request(
                f"{self.BASE_URL}/pools",
                headers=self.headers
            )

            if not pools:
                return False

            all_data = []
            for pool_id in pools:
                relays = APIUtils.make_request(
                    f"{self.BASE_URL}/pools/{pool_id}/relays",
                    headers=self.headers
                )
                if relays:
                    all_data.extend(relays)
                time.sleep(0.1)  # Rate limiting

            self.data = {'relays': all_data}
            print(f"Successfully fetched data with {len(all_data)} relays")
            print(f"Data is now: {self.data}")

            print("\nDebug: Data structure after fetch:")
            print(f"Keys in self.data: {self.data.keys()}")
            print(f"Number of relays: {len(self.data.get('relays', []))}")
            print("\nSample of first few relays:")
            for i, relay in enumerate(self.data.get('relays', [])[:3]):
                print(f"Relay {i + 1}: {relay}")
            return True
        except Exception as e:
            print(f"Error fetching data: {e}")
            return False

    def process_geographic_distribution(self):
        """Process node data to extract country distribution."""
        country_counts = Counter()

        try:
            with geoip2.database.Reader('data/GeoLite2-Country.mmdb') as reader:
                for relay in self.data['relays']:
                    try:
                        ip = relay.get('ipv4') or relay.get('ipv6')
                        if ip:
                            ip = ip.replace('[', '').replace(']', '')
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
        """Process Cardano relay data to extract hosting provider distribution."""
        if not self.data or 'relays' not in self.data:
            print("No relay data available")
            return pd.DataFrame(columns=['Category', 'Count'])

        hosting_counts = Counter()

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
                for relay in self.data['relays']:
                    try:
                        ip = relay.get('ipv4') or relay.get('ipv6')

                        if ip:
                            ip = ip.replace('[', '').replace(']', '')

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

                                if not provider_matched and not any(
                                        x in org.lower() for x in ['relay', 'pool', 'stake', 'r1', 'r2', 'mainnet']):
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
        if not self.data or 'relays' not in self.data:
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
                "Cardano geographic distribution",
                self.paths['plots']['geographic'],
                convert_countries=True
            )

            PlottingUtils.plot_pie_chart_with_filtered_legend(
                self.paths['data']['hosting'],
                "Cardano hosting distribution",
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

    parser = argparse.ArgumentParser(description='Cardano Network Analysis')
    parser.add_argument('--process-only', action='store_true', help='Only process data')
    parser.add_argument('--plot-only', action='store_true', help='Only create plots')
    args = parser.parse_args()

    analyzer = CardanoAnalyzer()

    if args.process_only:
        analyzer.fetch_data()
        analyzer.process_data()
    elif args.plot_only:
        analyzer.create_visualizations()
    else:
        analyzer.run_analysis()