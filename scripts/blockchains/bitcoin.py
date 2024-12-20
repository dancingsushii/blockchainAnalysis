from collections import Counter

import pandas as pd
from scripts.common.utils import PathManager, APIUtils
from scripts.common.plotting import PlottingUtils


class BitcoinAnalyzer:
    def __init__(self):
        self.paths = PathManager.get_paths()
        self.data = None
        self.processed_data = None
        self.ENDPOINT = 'https://bitnodes.io/api/v1/snapshots/latest/'

    def fetch_data(self):
        """Fetch Bitcoin node data from API."""
        self.data = APIUtils.make_request(self.ENDPOINT)
        if self.data:
            print(f"Successfully fetched data with {self.data.get('total_nodes', 0)} nodes")
            return True
        return False

    def process_client_distribution(self):
        """Process node data to extract base client distribution."""
        if not self.data or 'nodes' not in self.data:
            return pd.DataFrame(columns=['Category', 'Count'])

        client_counts = Counter()

        valid_clients = {
            "Satoshi": "Bitcoin Core",
            "btcwire": "btcd",
            "bcoin": "bcoin",
            "libbitcoin": "libbitcoin"
        }

        invalid_keywords = [
            "BTF", "CKCoinD", "Aurum", "Statoshi", "🙏", "Ladybug",
            "Classic", "ABC", "Unlimited", "Natasha"
        ]

        for node_info in self.data['nodes'].values():
            try:
                version_string = node_info[1]
                if not version_string:
                    continue

                version_clean = version_string.strip('/')

                if "Knots" in version_clean:
                    client_counts["Bitcoin Knots"] += 1
                    continue

                parts = version_clean.split(':')
                if len(parts) < 2:
                    continue

                client_name = parts[0]

                if any(keyword in version_clean for keyword in invalid_keywords):
                    continue

                if client_name in valid_clients:
                    client_counts[valid_clients[client_name]] += 1

            except Exception as e:
                continue

        df = pd.DataFrame(
            list(client_counts.items()),
            columns=['Category', 'Count']
        ).sort_values(by='Count', ascending=False)

        df = df[df['Count'] > 1]

        return df

    def process_data(self):
        """Process all distributions and save to CSV."""
        if not self.data or 'nodes' not in self.data:
            print("No data to process")
            return False

        country_counts = {}
        for node_info in self.data['nodes'].values():
            country = node_info[7]
            if country and country != "null" and country != "TOR":
                country_counts[country] = country_counts.get(country, 0) + 1

        self.processed_data = {
            'geographic': pd.DataFrame(
                list(country_counts.items()),
                columns=['Category', 'Count']
            ).sort_values(by='Count', ascending=False)
        }

        self.processed_data['geographic'].to_csv(self.paths['data']['geographic'], index=False)
        print(f"\nGeographic distribution saved to {self.paths['data']['geographic']}")
        print("\nTop 10 countries by node count:")
        print(self.processed_data['geographic'].head(10))

        self.processed_data['client'] = self.process_client_distribution()
        self.processed_data['client'].to_csv(self.paths['data']['client'], index=False)
        print(f"\nClient distribution saved to {self.paths['data']['client']}")
        print("\nTop 10 clients by node count:")
        print(self.processed_data['client'].head(10))


        hosting_counts = Counter()
        for node_info in self.data['nodes'].values():
            hosting = node_info[5] or 'Self-hosted/Unknown'

            # Skip Tor nodes
            if node_info[11] == "TOR":
                continue

            if hosting.endswith('.amazonaws.com'):
                hosting = 'Amazon Web Services'
            elif hosting.endswith('.googleusercontent.com'):
                hosting = 'Google Cloud Platform'
            elif 'hetzner' in hosting.lower():
                hosting = 'Hetzner'
            elif 'netcup' in hosting.lower():
                hosting = 'Netcup'

            hosting_counts[hosting] += 1

        self.processed_data['hosting'] = pd.DataFrame(
            list(hosting_counts.items()),
            columns=['Category', 'Count']
        ).sort_values(by='Count', ascending=False)

        # Filter out 'Self-hosted/Unknown'
        self.processed_data['hosting'] = self.processed_data['hosting'][
            self.processed_data['hosting']['Category'] != 'Self-hosted/Unknown'
            ]

        self.processed_data['hosting'].to_csv(self.paths['data']['hosting'], index=False)
        print(f"\nHosting distribution saved to {self.paths['data']['hosting']}")
        print("\nTop 10 hosting providers by node count:")
        print(self.processed_data['hosting'].head(10))

        return True

    def create_visualizations(self):
        """Create visualizations from processed data."""
        PlottingUtils.plot_pie_chart_with_filtered_legend(
            self.paths['data']['geographic'],
            "Bitcoin geographic distribution",
            self.paths['plots']['geographic'],
            True
        )
        print(f"Geographic plot saved as {self.paths['plots']['geographic']}")

        PlottingUtils.plot_pie_chart_with_filtered_legend(
            self.paths['data']['client'],
            "Bitcoin client distribution",
            self.paths['plots']['client'],
            False
        )
        print(f"Client plot saved as {self.paths['plots']['client']}")

        PlottingUtils.plot_pie_chart_with_filtered_legend(
            self.paths['data']['hosting'],
            "Bitcoin hosting distribution",
            self.paths['plots']['hosting'],
            False
        )
        print(f"Hosting plot saved as {self.paths['plots']['hosting']}")

    def run_analysis(self):
        """Run the complete analysis pipeline."""
        if self.fetch_data():
            if self.process_data():
                self.create_visualizations()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Bitcoin Network Analysis')
    parser.add_argument('--process-only', action='store_true', help='Only process data')
    parser.add_argument('--plot-only', action='store_true', help='Only create plots')
    args = parser.parse_args()

    analyzer = BitcoinAnalyzer()

    if args.process_only:
        analyzer.fetch_data()
        analyzer.process_data()
    elif args.plot_only:
        analyzer.create_visualizations()
    else:
        analyzer.run_analysis()
