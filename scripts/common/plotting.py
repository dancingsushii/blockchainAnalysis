import textwrap

import pandas as pd
import matplotlib.pyplot as plt
import os
from scripts.common.utils import CountryMapper


class PlottingUtils:
    @staticmethod
    def plot_pie_chart_with_filtered_legend(csv_path, title, output_path, convert_countries):
        """Create a pie chart with labels and legend for values > 1.5%."""
        colors = [
            '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
            '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf',
            '#aec7e8', '#ffbb78', '#98df8a',
        ]

        try:
            data = pd.read_csv(csv_path)

            if convert_countries:
                data['Category'] = data['Category'].apply(CountryMapper.get_country_name)


            data["Count"] = data["Count"].astype(int)
            total_count = data["Count"].sum()
            data["Percentage"] = (data["Count"] / total_count) * 100


            data = data[data['Category'].notna()]
            sorted_data = data.sort_values(by="Count", ascending=False)
            large_data = sorted_data[sorted_data["Percentage"] >= 1.5]
            others = sorted_data[sorted_data["Percentage"] < 1.5]["Count"].sum()

            if others > 0:
                others_row = pd.DataFrame({"Category": ["Others"], "Count": [others]})
                filtered_data = pd.concat([large_data, others_row])
                filtered_data = filtered_data.groupby('Category', as_index=False).sum()
            else:
                filtered_data = large_data


            filtered_data["Percentage"] = (filtered_data["Count"] / total_count) * 100
            wrapped_labels = filtered_data['Category'].apply(lambda x: "\n".join(textwrap.wrap(x, width=15)))

            plt.figure(figsize=(10, 10))
            wedges, texts, autotexts = plt.pie(
                filtered_data["Count"],
                labels=wrapped_labels,
                autopct=lambda pct: f"{pct:.1f}%" if pct >= 1.5 else "",
                startangle=140,
                colors=colors[:len(filtered_data)],
                labeldistance=1.1
            )

            plt.setp(texts, size=8)
            plt.setp(autotexts, size=8)

            legend_labels = [
                f"{category} ({percentage:.1f}%)"
                for category, percentage in zip(filtered_data["Category"], filtered_data["Percentage"])
                if percentage >= 1.5
            ]

            plt.legend(
                wedges[:len(legend_labels)],
                legend_labels,
                loc="upper center",
                bbox_to_anchor=(0.5, -0.1),
                ncol=3
            )

            plt.title(title)
            plt.axis('equal')
            plt.tight_layout()

            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            plt.savefig(output_path, bbox_inches='tight')
            plt.close()

        except Exception as e:
            print(f"Error in plot_pie_chart_with_filtered_legend: {e}")
            raise
