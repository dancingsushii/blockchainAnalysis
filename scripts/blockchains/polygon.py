import pandas as pd
from collections import Counter
from scripts.common.utils import PathManager
import geoip2.database


class PolygonAnalyzer:
    def __init__(self):
        self.paths = PathManager.get_paths()
        self.data = None
        self.processed_data = {
            'geographic': None,
            'client': None,
            'hosting': None
        }
        self.API_KEY = "F7CTIGM13IGZXNA8IH2SDY9BUY4XDP692G"
        self.BASE_URL = "https://api.polygonscan.com/api/nodes"

        self.NODE_TRACKER_URL = "https://polygonscan.com/nodetracker"

    def fetch_data(self):
        """Load Polygon node data from CSV file."""
        try:
            print("Available paths:", self.paths)
            base_path = self.paths.get('data', {}).get('base', '')
            if not base_path:
                print("Warning: Base path not found in self.paths")
                # Update with PathManager later
                base_path = "/Users/tetianayakovenko/Desktop/masterThesis/GitHubRepository/blockchainAnalysis/data"

            csv_path = f"{base_path}/raw/polygon/polygon_nodes.csv"
            print(f"Attempting to read from: {csv_path}")

            import os
            if not os.path.exists(csv_path):
                print(f"Error: File not found at {csv_path}")
                return False

            df = pd.read_csv(csv_path)

            nodes = []
            for _, row in df.iterrows():
                node = {
                    'ip': row['Host'],
                    'client': row['Client'],
                    'country': row['Country'],
                    'version': row['Client'],
                    'os': row['OS'],
                    'last_seen': row['Last Seen']
                }
                nodes.append(node)

            self.data = {'nodes': nodes}
            print(f"Successfully loaded {len(nodes)} nodes from CSV")
            return True

        except Exception as e:
            print(f"Error loading data: {e}")
            import traceback
            print("Full traceback:")
            print(traceback.format_exc())
            return False

    def process_client_distribution(self):
        """Process node data to extract Polygon client distribution."""
        if not self.data or 'nodes' not in self.data:
            return pd.DataFrame(columns=['Category', 'Count', 'Percentage'])

        polygon_clients = [
            'bor',
            'Worldland',
            'CoreGeth',
            'reth',
            'Gqdc',
            'Ronin'
        ]

        polygon_client_counts = Counter()

        for node in self.data['nodes']:
            if node.get('client'):
                client = node['client'].split('/')[0].strip()
                if client in polygon_clients:
                    polygon_client_counts[client] += 1

        df = pd.DataFrame(
            list(polygon_client_counts.items()),
            columns=['Category', 'Count']
        ).sort_values(by='Count', ascending=False)

        total = df['Count'].sum()
        df['Percentage'] = (df['Count'] / total * 100).round(2)

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
                            ip = node['ip']
                            response = reader.asn(ip)
                            org = response.autonomous_system_organization
                            if org:
                                # Clean up organization names
                                org = (org.replace(' LLC', '')
                                       .replace(' Ltd.', '')
                                       .replace(' Inc.', '')
                                       .replace(' Corporation', '')
                                       .replace(' Corp.', '')
                                       .replace(' SA', '')
                                       .replace(' AG', '')
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

        total = df['Count'].sum()
        df['Percentage'] = (df['Count'] / total * 100).round(2)

        return df

    def process_geographic_distribution(self):
        """Process node data to extract country distribution."""
        if not self.data or 'nodes' not in self.data:
            return pd.DataFrame(columns=['Category', 'Count'])

        country_counts = Counter()

        for node in self.data['nodes']:
            if node.get('country'):
                country_counts[node['country']] += 1

        df = pd.DataFrame(
            list(country_counts.items()),
            columns=['Category', 'Count']
        ).sort_values(by='Count', ascending=False)

        total = df['Count'].sum()
        df['Percentage'] = (df['Count'] / total * 100).round(2)

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

        self.processed_data['client'] = self.process_client_distribution()
        self.processed_data['client'].to_csv(self.paths['data']['client'], index=False)
        print("\nTop 10 clients by node count:")
        print(self.processed_data['client'].head(10))

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
                "Polygon geographic distribution",
                self.paths['plots']['geographic'],
                convert_countries=True
            )

            PlottingUtils.plot_pie_chart_with_filtered_legend(
                self.paths['data']['client'],
                "Polygon client distribution",
                self.paths['plots']['client'],
                convert_countries=False
            )

            PlottingUtils.plot_pie_chart_with_filtered_legend(
                self.paths['data']['hosting'],
                "Polygon hosting distribution",
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

    parser = argparse.ArgumentParser(description='Polygon Network Analysis')
    parser.add_argument('--process-only', action='store_true', help='Only process data')
    parser.add_argument('--plot-only', action='store_true', help='Only create plots')
    args = parser.parse_args()

    analyzer = PolygonAnalyzer()

    if args.process_only:
        analyzer.fetch_data()
        analyzer.process_data()
    elif args.plot_only:
        analyzer.create_visualizations()
    else:
        analyzer.run_analysis()