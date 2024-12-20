import pandas as pd
import matplotlib.pyplot as plt
import os


def create_geographic_chart(csv_path, blockchain_name):
    """
    Create a horizontal bar chart for geographic distribution of a blockchain.

    Args:
        csv_path (str): Path to the CSV file containing geographic distribution data
        blockchain_name (str): Name of the blockchain for the title
    """
    df = pd.read_csv(csv_path)
    df = df.sort_values(by='percentage', ascending=True)
    plt.figure(figsize=(12, 6))

    bars = plt.barh(df['country'], df['percentage'])

    plt.title(f'{blockchain_name} Geographic Distribution')
    plt.xlabel('Percentage (%)')
    plt.ylabel('Country')

    for bar in bars:
        width = bar.get_width()
        plt.text(width, bar.get_y() + bar.get_height() / 2,
                 f'{width:.1f}%',
                 ha='left', va='center', fontsize=8)

    plt.tight_layout()

    output_dir = 'plots/geographic_distribution'
    os.makedirs(output_dir, exist_ok=True)
    plt.savefig(f'{output_dir}/{blockchain_name.lower()}_geographic_distribution.pdf')
    plt.close()


def main():
    base_dir = 'data/processed/blockchains'
    blockchain_dirs = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]

    for blockchain in blockchain_dirs:
        geo_file = os.path.join(base_dir, blockchain, 'geographic_distribution.csv')

        if os.path.exists(geo_file):
            print(f"Processing {blockchain}...")
            create_geographic_chart(geo_file, blockchain.capitalize())
        else:
            print(f"No geographic distribution data found for {blockchain}")


if __name__ == "__main__":
    main()
