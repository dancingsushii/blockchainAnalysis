import json
from datetime import datetime
import pandas as pd
from scripts.common.utils import PathManager, APIUtils


class TezosAnalyzer:
    def __init__(self):
        self.paths = PathManager.get_paths()
        self.data = None
        self.processed_data = {
            'geographic': None,
            'hosting': None
        }
        self.ENDPOINT = 'https://services.tzkt.io/v1/nodes/stats'

    def fetch_data(self):
        """Fetch Tezos node data from API."""
        response = APIUtils.make_request(self.ENDPOINT)
        if response:
            self.data = response
            print(f"Successfully fetched node statistics data")
            return True
        print("Failed to fetch data")
        return False

    def process_geographic_distribution(self):
        """Process node data to extract country distribution."""
        if not self.data or 'heatmap' not in self.data:
            return pd.DataFrame(columns=['Category', 'Count'])

        geographic_data = [
            {'Category': item['countryCode'], 'Count': item['count']}
            for item in self.data['heatmap']
            if 'countryCode' in item and 'count' in item
        ]

        return pd.DataFrame(geographic_data).sort_values(by='Count', ascending=False)


    def process_hosting_distribution(self):
        """Process node data to extract hosting provider distribution."""
        if not self.data or 'topHosting' not in self.data:
            return pd.DataFrame(columns=['Category', 'Count'])

        hosting_data = [
            {'Category': item['hosting'], 'Count': item['count']}
            for item in self.data['topHosting']
            if 'hosting' in item and 'count' in item
        ]

        return pd.DataFrame(hosting_data).sort_values(by='Count', ascending=False)

    def process_data(self):
        """Process all distributions and save to CSV."""
        if not self.data:
            print("No data to process")
            return False

        self.processed_data['geographic'] = self.process_geographic_distribution()
        if not self.processed_data['geographic'].empty:
            self.processed_data['geographic'].to_csv(self.paths['data']['geographic'], index=False)
            print(f"\nGeographic distribution saved to {self.paths['data']['geographic']}")
            print("\nTop 10 countries by node count:")
            print(self.processed_data['geographic'].head(10))


        self.processed_data['hosting'] = self.process_hosting_distribution()
        if not self.processed_data['hosting'].empty:
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
                "Tezos geographic distribution",
                self.paths['plots']['geographic'],
                convert_countries=True
            )


            PlottingUtils.plot_pie_chart_with_filtered_legend(
                self.paths['data']['hosting'],
                "Tezos hosting distribution",
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

    parser = argparse.ArgumentParser(description='Tezos Network Analysis')
    parser.add_argument('--process-only', action='store_true', help='Only process data')
    parser.add_argument('--plot-only', action='store_true', help='Only create plots')
    args = parser.parse_args()

    analyzer = TezosAnalyzer()

    if args.process_only:
        analyzer.fetch_data()
        analyzer.process_data()
    elif args.plot_only:
        analyzer.create_visualizations()
    else:
        analyzer.run_analysis()