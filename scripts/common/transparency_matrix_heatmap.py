import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns


def create_transparency_heatmap(scores, blockchains, criteria, output_file):
    """
    Create a heatmap visualization of the blockchain transparency analysis matrix.

    Args:
        scores (np.array): Matrix of scores for each blockchain and criterion
        blockchains (list): List of blockchain names
        criteria (list): List of criteria names
        output_file (str): Path for saving the output file
    """
    plt.figure(figsize=(12, 10))

    sns.heatmap(scores,
                annot=True,
                fmt='d',
                cmap='YlOrRd',
                xticklabels=criteria,
                yticklabels=blockchains,
                cbar_kws={'label': 'Score'})

    plt.title('Blockchain Transparency Analysis Matrix', pad=20)
    plt.xlabel('Criteria', labelpad=10)
    plt.ylabel('Blockchains', labelpad=10)

    plt.xticks(rotation=0)
    plt.yticks(rotation=0)

    plt.tight_layout()

    plt.savefig(output_file, bbox_inches='tight', dpi=300)
    plt.close()


def create_ranking_barchart(scores, blockchains, output_file):
    """
    Create a bar chart showing total scores for each blockchain, sorted from highest to lowest.
    """
    total_scores = np.sum(scores, axis=1)

    sorted_indices = np.argsort(total_scores)[::-1]
    sorted_scores = total_scores[sorted_indices]
    sorted_names = [blockchains[i] for i in sorted_indices]

    max_score = np.max(total_scores)
    colors = plt.cm.YlOrRd(sorted_scores / max_score)


    fig, ax = plt.subplots(figsize=(12, 6))
    bars = ax.bar(range(len(sorted_scores)), sorted_scores, color=colors)

    ax.set_ylabel('Total score')

    ax.set_xticks(range(len(sorted_scores)))
    ax.set_xticklabels(sorted_names, rotation=45, ha='right')

    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2., height,
                f'{int(height)}',
                ha='center', va='bottom')

    sm = plt.cm.ScalarMappable(cmap=plt.cm.YlOrRd, norm=plt.Normalize(vmin=0, vmax=max_score))
    sm.set_array([])
    fig.colorbar(sm, ax=ax, label='Score')
    plt.tight_layout()

    plt.savefig(output_file, bbox_inches='tight', dpi=300)
    plt.close()


def main():
    blockchains = ['Ethereum', 'Ethereum Classic', 'Bitcoin', 'Bitcoin Cash', 'Litecoin',
                   'Dogecoin', 'Cardano', 'Solana', 'Polkadot', 'Polygon', 'Algorand',
                   'Tezos', 'NEAR', 'Stellar', 'Ripple']

    criteria = ['Data\nAvailability', 'Data\nProcessability', 'Collection\nMethods',
                'Data\nCompleteness', 'Verification\nCapabilities']

    scores = np.array([
        [3, 3, 3, 3, 3],  # Ethereum
        [1, 1, 2, 3, 1],  # Ethereum Classic
        [3, 3, 2, 3, 3],  # Bitcoin
        [2, 1, 2, 2, 1],  # Bitcoin Cash
        [2, 1, 2, 2, 2],  # Litecoin
        [2, 1, 2, 2, 2],  # Dogecoin
        [1, 1, 1, 1, 2],  # Cardano
        [1, 1, 2, 1, 2],  # Solana
        [3, 3, 3, 3, 2],  # Polkadot
        [0, 0, 0, 2, 1],  # Polygon
        [3, 2, 3, 3, 2],  # Algorand
        [2, 2, 3, 3, 2],  # Tezos
        [1, 1, 1, 1, 2],  # NEAR
        [2, 2, 3, 3, 2],  # Stellar
        [1, 1, 1, 1, 1],  # Ripple
    ])

    create_transparency_heatmap(scores, blockchains, criteria, 'transparency_heatmap.png')
    create_ranking_barchart(scores, blockchains, 'transparency_ranking.png')


if __name__ == "__main__":
    main()