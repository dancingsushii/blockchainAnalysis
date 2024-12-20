import requests
import json
import os
from typing import Dict, Optional
import logging
from datetime import datetime
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BlockchainNodesFetcher:
    def __init__(self, api_keys: Dict[str, str]):
        """Initialize fetcher with API keys"""
        self.api_keys = api_keys
        self.output_dir = "data/processed/node_counts"
        os.makedirs(self.output_dir, exist_ok=True)

    def _make_request(self, url: str, method: str = "GET", **kwargs) -> Optional[Dict]:
        """Base method for making HTTP requests with retries"""
        retries = 3
        for attempt in range(retries):
            try:
                response = requests.request(method=method, url=url, timeout=10, **kwargs)
                response.raise_for_status()
                return response.json()
            except requests.RequestException as e:
                if attempt == retries - 1:
                    logger.error(f"Request error for {url}: {e}")
                    return None
                time.sleep(1 * (attempt + 1))
            except json.JSONDecodeError as e:
                logger.error(f"JSON parsing error for {url}: {e}")
                return None

    def fetch_tezos_nodes(self) -> Optional[int]:
        """Fetch Tezos node count"""
        data = self._make_request("https://services.tzkt.io/v1/nodes/stats")
        if data and 'heatmap' in data:
            total_nodes = sum(item['count'] for item in data['heatmap'])
            logger.info(f"Successfully fetched Tezos node count: {total_nodes}")
            return total_nodes
        return None

    def fetch_ethereum_nodes(self) -> Optional[int]:
        """Fetch Ethereum node count"""
        if 'etherscan' not in self.api_keys:
            logger.error("Etherscan API key not provided")
            return None

        data = self._make_request(
            url="https://api.etherscan.io/api",
            params={
                "module": "stats",
                "action": "nodecount",
                "apikey": self.api_keys['etherscan']
            }
        )

        if data and data.get('status') == '1' and 'result' in data:
            count = int(data['result']['TotalNodeCount'])
            logger.info(f"Successfully fetched Ethereum node count: {count}")
            return count
        return None

    def fetch_bitcoin_nodes(self) -> Optional[int]:
        """Fetch Bitcoin node count"""
        data = self._make_request("https://bitnodes.io/api/v1/snapshots/latest/")
        if data and 'total_nodes' in data:
            count = int(data['total_nodes'])
            logger.info(f"Successfully fetched Bitcoin node count: {count}")
            return count
        return None

    def fetch_eth_classic_nodes(self) -> Optional[int]:
        """Fetch Ethereum Classic node count"""
        data = self._make_request("https://api.etcnodes.org/peers", params={"all": "true"})
        if data:
            count = len(data)
            logger.info(f"Successfully fetched ETC node count: {count}")
            return count
        return None

    def fetch_cardano_nodes(self) -> Optional[int]:
        """Fetch Cardano node count"""
        all_pools = []
        page = 1

        while True:
            data = self._make_request(
                url="https://cardano-mainnet.blockfrost.io/api/v0/pools/extended",
                headers={"project_id": "mainnetR26eS04QwL3UxF81KSzq4Gj4VsnNn3lO"},
                params={"count": 100, "page": page}
            )

            if not data:
                break

            all_pools.extend(data)
            if len(data) < 100:
                break

            page += 1

        if all_pools:
            count = len(all_pools)
            logger.info(f"Successfully fetched Cardano node count: {count}")
            return count
        return None

    def fetch_solana_nodes(self) -> Optional[int]:
        """Fetch Solana node count"""
        data = self._make_request(
            url="https://api.mainnet-beta.solana.com",
            method="POST",
            headers={'Content-Type': 'application/json'},
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getClusterNodes"
            }
        )

        if data and 'result' in data:
            count = len(data['result'])
            logger.info(f"Successfully fetched Solana node count: {count}")
            return count
        return None

    def fetch_near_nodes(self) -> Optional[int]:
        """Fetch NEAR node count"""
        data = self._make_request("https://rpc.mainnet.near.org/network_info")
        if data and 'active_peers' in data:
            count = len(data['active_peers'])
            logger.info(f"Successfully fetched NEAR node count: {count}")
            return count
        return None

    def fetch_stellar_nodes(self) -> Optional[int]:
        """Fetch Stellar node count"""
        data = self._make_request("https://api.stellarbeat.io/v1/node")
        if data:
            count = len(data)
            logger.info(f"Successfully fetched Stellar node count: {count}")
            return count
        return None

    def fetch_bitcoin_cash_nodes(self) -> Optional[int]:
        """Fetch Bitcoin Cash node count"""
        data = self._make_request("https://api.blockchair.com/bitcoin-cash/nodes")
        if data and 'data' in data and 'nodes' in data['data']:
            count = len(data['data']['nodes'])
            logger.info(f"Successfully fetched Bitcoin Cash node count: {count}")
            return count
        return None

    def fetch_dogecoin_nodes(self) -> Optional[int]:
        """Fetch Dogecoin node count"""
        data = self._make_request("https://api.blockchair.com/dogecoin/nodes")
        if data and 'data' in data and 'nodes' in data['data']:
            count = len(data['data']['nodes'])
            logger.info(f"Successfully fetched Dogecoin node count: {count}")
            return count
        return None

    def fetch_litecoin_nodes(self) -> Optional[int]:
        """Fetch Litecoin node count"""
        data = self._make_request("https://api.blockchair.com/litecoin/nodes")
        if data and 'data' in data and 'nodes' in data['data']:
            count = len(data['data']['nodes'])
            logger.info(f"Successfully fetched Litecoin node count: {count}")
            return count
        return None

    def fetch_ripple_nodes(self) -> Optional[int]:
        """Fetch Ripple node count"""
        data = self._make_request(
            url="https://go.getblock.io/e97ec62471c54056a22778385f50ee0e",
            method="POST",
            headers={
                'Content-Type': 'application/json',
                'x-api-key': 'e97ec62471c54056a22778385f50ee0e'
            },
            json={
                "jsonrpc": "2.0",
                "method": "server_info",
                "params": [],
                "id": "xrp-e97ec"
            }
        )
        if data and 'result' in data and 'info' in data['result']:
            count = data['result']['info'].get('peers', 0)
            logger.info(f"Successfully fetched Ripple node count: {count}")
            return count
        return None

    def fetch_algorand_nodes(self) -> Optional[int]:
        """Fetch Algorand node count"""

        # Unfortunately, for Algorand we found the Nodely service the most reliable source
        # Data is available only via manual export
        file_path = '/Users/tetianayakovenko/Desktop/masterThesis/GitHubRepository/blockchainAnalysis/data/raw/algorand/algorand_nodes.csv'
        try:
            with open(file_path, 'r') as file:
                lines = file.readlines()

                if not lines:
                    return None

                last_line = lines[-1].strip()
                values = last_line.split(',')
                if len(values) >= 3:
                    logger.info(f"Successfully fetched Algorand node count: {int(values[2])}")
                    return int(values[2])
                else:
                    return None
        except Exception as e:
            print(f"Error reading the file: {e}")
            return None

    def fetch_polkadot_nodes(self) -> Optional[int]:
        """
        Fetch the total number of Polkadot nodes from the `nebula_crawl.json` file.
        """
        try:
            file_path = '/Users/tetianayakovenko/Desktop/masterThesis/GitHubRepository/blockchainAnalysis/data/raw/polkadot/nebula_crawl.json'

            with open(file_path, 'r') as file:
                data = json.load(file)

            if "crawled_peers" in data:
                total_nodes = data["crawled_peers"]
                logger.info(f"Successfully fetched Polkadot nodes: {total_nodes}")
                return total_nodes
            else:
                print("Parameter 'crawled_peers' not found in the JSON file.")
                return None

        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return None


    def fetch_polygon_nodes(self) -> Optional[int]:
        """Fetch Polygon node count"""
        # Unfortunately, for Polygon we found the polygonscan nodetracker service the most reliable source
        # Data is available only via manual export

        # Change to PathManager later
        file_path = '/Users/tetianayakovenko/Desktop/masterThesis/GitHubRepository/blockchainAnalysis/data/raw/polygon/polygon_nodes.csv'
        try:
            with open(file_path, 'r') as file:
                lines = file.readlines()

                logger.info(f"Successfully fetched Polygon node count: {len(lines)}")
                return len(lines)

        except Exception as e:
            print(f"Error reading the file: {e}")
            return len(lines)

    def fetch_all_nodes(self) -> Dict[str, Optional[int]]:
        """Fetch all node counts"""
        fetchers = {
            "Algorand": self.fetch_algorand_nodes,
            "Bitcoin": self.fetch_bitcoin_nodes,
            "Bitcoin Cash": self.fetch_bitcoin_cash_nodes,
            "Cardano": self.fetch_cardano_nodes,
            "Dogecoin": self.fetch_dogecoin_nodes,
            "Ethereum": self.fetch_ethereum_nodes,
            "Ethereum Classic": self.fetch_eth_classic_nodes,
            "Litecoin": self.fetch_litecoin_nodes,
            "NEAR": self.fetch_near_nodes,
            "Polkadot": self.fetch_polkadot_nodes,
            "Polygon": self.fetch_polygon_nodes,
            "Ripple": self.fetch_ripple_nodes,
            "Solana": self.fetch_solana_nodes,
            "Stellar": self.fetch_stellar_nodes,
            "Tezos": self.fetch_tezos_nodes,
        }

        node_counts = {}
        for name, fetcher in fetchers.items():
            try:
                if count := fetcher():
                    node_counts[name] = count
            except Exception as e:
                logger.error(f"Error fetching {name} data: {e}")

        return node_counts

    def save_data(self, data: Dict[str, int]):
        """Save data to JSON file"""
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_file = os.path.join(self.output_dir, f"node_counts_{timestamp}.json")

        try:
            with open(output_file, 'w') as f:
                json.dump({
                    "timestamp": timestamp,
                    "data": data,
                    "fetch_time": datetime.now().isoformat()
                }, f, indent=4)
            logger.info(f"Data successfully saved to {output_file}")
        except IOError as e:
            logger.error(f"Error saving data: {e}")


def main():
    api_keys = {
        "etherscan": os.getenv("ETHERSCAN_API_KEY", "ZTHTDCDGHAU68XJ9UD9MYV2VR5AY2V7J7Y")
    }

    try:
        fetcher = BlockchainNodesFetcher(api_keys)
        node_counts = fetcher.fetch_all_nodes()

        if node_counts:
            fetcher.save_data(node_counts)
            logger.info("Data collection completed successfully")
        else:
            logger.warning("No data was collected")

    except Exception as e:
        logger.error(f"Unexpected error during data collection: {e}")
        raise


if __name__ == "__main__":
    main()