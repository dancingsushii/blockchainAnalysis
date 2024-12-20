import json
from datetime import datetime
import pandas as pd
from collections import Counter
from scripts.common.utils import PathManager, APIUtils, CountryMapper


class EthereumClassicAnalyzer:
    def __init__(self):
        self.paths = PathManager.get_paths()
        self.data = None
        self.processed_data = {
            'geographic': None,
            'client': None,
            'hosting': None
        }
        self.ENDPOINT = 'https://api.etcnodes.org/peers'
        self.country_mapper = CountryMapper()

    def fetch_data(self):
        """Fetch Ethereum Classic node data from API."""
        response = APIUtils.make_request(self.ENDPOINT)
        if response:
            self.data = response  # API returns list of nodes directly
            print(f"Successfully fetched data with {len(self.data)} nodes")
            return True
        return False

    def process_geographic_distribution(self):
        """Process node data to extract country distribution."""
        country_counts = Counter()

        for node in self.data:
            try:
                if 'ip_info' in node and 'countryCode' in node['ip_info']:
                    country = node['ip_info']['countryCode']
                    if country:
                        country_counts[country] += 1
            except Exception as e:
                continue

        return pd.DataFrame(
            list(country_counts.items()),
            columns=['Category', 'Count']
        ).sort_values(by='Count', ascending=False)

    def process_client_distribution(self):
        """Process node data to extract client distribution."""
        client_counts = Counter()

        for node in self.data:
            try:
                if 'name' in node and node['name']:
                    client = node['name'].split('/')[0]  # Get main client name
                    client_counts[client] += 1
            except Exception as e:
                continue

        return pd.DataFrame(
            list(client_counts.items()),
            columns=['Category', 'Count']
        ).sort_values(by='Count', ascending=False)

    def process_hosting_distribution(self):
        """Process node data to extract hosting provider distribution by organization."""
        hosting_counts = Counter()

        for node in self.data:
            try:
                if 'ip_info' in node and 'org' in node['ip_info']:
                    org = node['ip_info']['org']
                    if org:
                        # Extract the main provider name (everything after "AS{number} ")
                        provider = org.split(' ', 1)[1] if ' ' in org else org
                        hosting_counts[provider] += 1
                else:
                    hosting_counts['Unknown'] += 1
            except Exception as e:
                hosting_counts['Unknown'] += 1
                continue

        # Convert to DataFrame
        if hosting_counts:
            df = pd.DataFrame(
                list(hosting_counts.items()),
                columns=['Category', 'Count']
            ).sort_values(by='Count', ascending=False)

            # Calculate percentage
            total = df['Count'].sum()
            df['Percentage'] = (df['Count'] / total * 100).round(2)

            # Filter out values less than 3.3% and group them into "Others"
            others_threshold = 3.3
            others_df = df[df['Percentage'] < others_threshold]
            others_count = others_df['Count'].sum()
            others_percentage = (others_count / total * 100).round(2)

            main_df = df[df['Percentage'] >= others_threshold]
            others_row = pd.DataFrame(
                {'Category': ['Others'], 'Count': [others_count], 'Percentage': [others_percentage]})
            main_df = pd.concat([main_df, others_row], ignore_index=True)

            return main_df
        else:
            print("Warning: No hosting provider data could be collected")
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
        print("\nTop hostings by node count:")
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
                "Ethereum Classic geographic distribution",
                self.paths['plots']['geographic'],
                convert_countries=True
            )

            PlottingUtils.plot_pie_chart_with_filtered_legend(
                self.paths['data']['client'],
                "Ethereum Classic client distribution",
                self.paths['plots']['client'],
                convert_countries=False
            )

            PlottingUtils.plot_pie_chart_with_filtered_legend(
                self.paths['data']['hosting'],
                "Ethereum Classic hosting distribution",
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

    parser = argparse.ArgumentParser(description='Ethereum Classic Network Analysis')
    parser.add_argument('--process-only', action='store_true', help='Only process data')
    parser.add_argument('--plot-only', action='store_true', help='Only create plots')
    args = parser.parse_args()

    analyzer = EthereumClassicAnalyzer()

    if args.process_only:
        analyzer.fetch_data()
        analyzer.process_data()
    elif args.plot_only:
        analyzer.create_visualizations()
    else:
        analyzer.run_analysis()