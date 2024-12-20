import json
import os
import matplotlib.pyplot as plt
import glob


def load_latest_data():
    """Load the most recent data file from the data directory"""
    # Change to PathManager later
    data_files = glob.glob('/Users/tetianayakovenko/Desktop/masterThesis/GitHubRepository/blockchainAnalysis/data/processed/node_counts/node_counts_*.json')
    if not data_files:
        raise FileNotFoundError("No data files found")

    latest_file = max(data_files)
    with open(latest_file, 'r') as f:
        return json.load(f)


def create_bar_chart(data):
    """Create a bar chart from the node count data, sorted from largest to smallest"""
    sorted_data = dict(sorted(data['data'].items(), key=lambda item: item[1], reverse=True))

    blockchains = list(sorted_data.keys())
    node_counts = list(sorted_data.values())

    fig, ax = plt.subplots(figsize=(12, 8))
    bars = ax.bar(blockchains, node_counts, width=0.8)
    ax.set_ylabel('Number of nodes')

    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2., height,
                 f'{int(height):,}',
                 ha='center', va='bottom')


    ax.set_xticklabels(blockchains, ha='right', rotation=45, multialignment='right')
    plt.tight_layout()

    output_file = f"plots/node_counts/node_counts_{data['timestamp']}.png"
    os.makedirs('plots', exist_ok=True)
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Plot saved to {output_file}")


def main():
    try:
        data = load_latest_data()

        create_bar_chart(data)

    except Exception as e:
        print(f"Error creating plot: {e}")


if __name__ == "__main__":
    main()