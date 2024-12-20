from scripts.common.plotting import PlottingUtils
from scripts.common.utils import PathManager
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AlgorandAnalyzer:
    def __init__(self):
        self.paths = PathManager.get_paths()
        self.data = None

    def create_visualizations(self):
        """Create visualizations from processed data."""
        PlottingUtils.plot_pie_chart_with_filtered_legend(
            self.paths['data']['geographic'],
            "Algorand geographic distribution",
            self.paths['plots']['geographic'],
            True
        )
        print(f"Plot saved as {self.paths['plots']['geographic']}")

        PlottingUtils.plot_pie_chart_with_filtered_legend(
            self.paths['data']['hosting'],
            "Algorand hosting distribution",
            self.paths['plots']['hosting'],
            True
        )
        print(f"Plot saved as {self.paths['plots']['hosting']}")



# class AlgorandDBFetcher:
#     def __init__(self):
#         self.conn_string = (
#             "postgresql://root@127.0.0.1:26257/defaultdb?"
#             "sslmode=disable"
#         )
#         self.conn = None
#
#     def connect(self):
#         """Establish database connection"""
#         try:
#             self.conn = psycopg2.connect(self.conn_string)
#             logger.info("Successfully connected to CockroachDB")
#         except Exception as e:
#             logger.error(f"Error connecting to database: {e}")
#             raise
#
#     def fetch_tables(self) -> List[str]:
#         """Get list of tables in database"""
#         try:
#             with self.conn.cursor() as cur:
#                 cur.execute("""
#                     SELECT table_name
#                     FROM information_schema.tables
#                     WHERE table_schema = 'public'
#                 """)
#                 return [table[0] for table in cur.fetchall()]
#         except Exception as e:
#             logger.error(f"Error fetching tables: {e}")
#             return []
#
#     def fetch_node_data(self) -> List[Dict[Any, Any]]:
#         """Fetch node data from database"""
#         try:
#             with self.conn.cursor() as cur:
#                 # Adjust query based on your table structure
#                 cur.execute("""
#                     SELECT * FROM nodes
#                     WHERE active = true
#                     ORDER BY last_seen DESC
#                 """)
#                 columns = [desc[0] for desc in cur.description]
#                 results = []
#                 for row in cur.fetchall():
#                     results.append(dict(zip(columns, row)))
#                 return results
#         except Exception as e:
#             logger.error(f"Error fetching node data: {e}")
#             return []
#
#     def close(self):
#         """Close database connection"""
#         if self.conn:
#             self.conn.close()
#             logger.info("Database connection closed")


if __name__ == "__main__":

    analyzer = AlgorandAnalyzer()
    # fetcher = AlgorandDBFetcher()

    # try:
    #     fetcher.connect()
    #
    #     tables = fetcher.fetch_tables()
    #     logger.info(f"Available tables: {tables}")
    #
    #     nodes = fetcher.fetch_node_data()
    #     logger.info(f"Found {len(nodes)} active nodes")
    #
    #     for node in nodes:
    #         print(node)
    #
    # except Exception as e:
    #     logger.error(f"Error in main: {e}")
    # finally:
    #     fetcher.close()

    analyzer.create_visualizations()
