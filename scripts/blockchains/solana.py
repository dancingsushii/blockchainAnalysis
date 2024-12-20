import json
import os
import subprocess
from datetime import datetime
import pandas as pd
from collections import Counter
from scripts.common.utils import PathManager, APIUtils
import geoip2.database
import socket


class SolanaAnalyzer:
    def __init__(self):
        self.paths = PathManager.get_paths()
        self.data = None
        self.processed_data = {
            'geographic': None,
            'client': None,
            'hosting': None
        }
        self.ENDPOINT = "https://api.mainnet-beta.solana.com"
        self.headers = {'Content-Type': 'application/json'}

    def fetch_data(self):
        """Fetch Solana node data from API."""
        response = APIUtils.make_request(
            url=self.ENDPOINT,
            method='POST',
            headers=self.headers,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getClusterNodes"
            }
        )

        if response and 'result' in response:
            self.data = response['result']
            print(f"Successfully fetched data with {len(self.data)} nodes")
            return True
        print("Failed to fetch data or invalid data format")
        return False

    def process_geographic_distribution(self):
        """Process node data to extract country distribution."""
        country_counts = Counter()

        try:
            with geoip2.database.Reader('data/GeoLite2-Country.mmdb') as reader:
                for node in self.data:
                    try:
                        ip = node.get('gossip', '').split(':')[0]

                        try:
                            socket.inet_aton(ip)
                        except socket.error:
                            continue

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

    def process_client_distribution(self):
        """Process node data to extract client distribution using both API and CLI."""
        client_counts = Counter()

        solana_paths = [
            "~/.local/share/solana/install/active_release/bin/solana",
            "~/.local/bin/solana",
            "/usr/local/bin/solana",
            os.path.expanduser("~/.local/share/solana/install/active_release/bin/solana"),
            os.path.expanduser("~/.local/bin/solana"),
            "/home/tetiana/.local/share/solana/install/active_release/bin/solana"
        ]

        solana_executable = None
        for path in solana_paths:
            expanded_path = os.path.expanduser(path)
            if os.path.exists(expanded_path):
                solana_executable = expanded_path
                print(f"\nDebug: Found Solana CLI at: {expanded_path}")
                break

        try:
            if solana_executable:
                result = subprocess.run([solana_executable, "validator-info", "get", "--output", "json"],
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE,
                                        text=True)

                if result.stderr:
                    print(f"CLI Warning: {result.stderr}")

                if result.stdout:
                    validator_info = json.loads(result.stdout)
                    print(f"\nDebug: Found {len(validator_info)} validators from CLI")

                    validator_info_map = {info['identityPubkey']: info['info'] for info in validator_info}

                    for node in self.data:
                        try:
                            pubkey = node.get('pubkey')
                            version = node.get('version', 'Unknown')

                            if pubkey in validator_info_map:
                                info = validator_info_map[pubkey]
                                if 'details' in info and any(client in info['details'].lower() for client in
                                                             ['jito', 'mev', 'firedancer', 'jump']):
                                    if 'jito' in info['details'].lower() or 'mev' in info['details'].lower():
                                        client_counts['Jito Labs'] += 1
                                    elif 'firedancer' in info['details'].lower() or 'jump' in info['details'].lower():
                                        client_counts['Firedancer'] += 1
                                    continue

                            if version:
                                if any(client in version.lower() for client in ['jito', 'mev']):
                                    client_counts['Jito Labs'] += 1
                                elif any(client in version.lower() for client in ['firedancer', 'jump']):
                                    client_counts['Firedancer'] += 1
                                else:
                                    client_counts['Solana Labs'] += 1

                        except Exception as e:
                            print(f"Error processing node {pubkey}: {str(e)}")
                            client_counts['Unknown'] += 1
                else:
                    print("No output from validator-info command")
                    raise Exception("No validator info output")

            else:
                raise Exception("Solana CLI not found in common paths")

        except Exception as e:
            print(f"Error accessing Solana CLI: {str(e)}")
            print("Falling back to API-only processing")

            for node in self.data:
                try:
                    version = node.get('version', 'Unknown')

                    if version:
                        if any(client in version.lower() for client in ['jito', 'mev']):
                            client_counts['Jito Labs'] += 1
                        elif any(client in version.lower() for client in ['firedancer', 'jump']):
                            client_counts['Firedancer'] += 1
                        elif 'MEV' in version or 'Custom' in version:
                            client_counts['Custom/MEV Client'] += 1
                        else:
                            client_counts['Solana Labs'] += 1
                    else:
                        client_counts['Unknown'] += 1

                except Exception:
                    client_counts['Unknown'] += 1

        df = pd.DataFrame(
            list(client_counts.items()),
            columns=['Category', 'Count']
        ).sort_values(by='Count', ascending=False)

        total = df['Count'].sum()
        df['Percentage'] = (df['Count'] / total * 100).round(2)

        return df

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
                        ip = node.get('gossip', '').split(':')[0]

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
                "Solana geographic distribution",
                self.paths['plots']['geographic'],
                convert_countries=True
            )

            PlottingUtils.plot_pie_chart_with_filtered_legend(
                self.paths['data']['client'],
                "Solana client distribution",
                self.paths['plots']['client'],
                convert_countries=False
            )

            PlottingUtils.plot_pie_chart_with_filtered_legend(
                self.paths['data']['hosting'],
                "Solana hosting distribution",
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

    parser = argparse.ArgumentParser(description='Solana Network Analysis')
    parser.add_argument('--process-only', action='store_true', help='Only process data')
    parser.add_argument('--plot-only', action='store_true', help='Only create plots')
    args = parser.parse_args()

    analyzer = SolanaAnalyzer()

    if args.process_only:
        analyzer.fetch_data()
        analyzer.process_data()
    elif args.plot_only:
        analyzer.create_visualizations()
    else:
        analyzer.run_analysis()