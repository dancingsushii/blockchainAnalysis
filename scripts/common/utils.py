from pathlib import Path
import os
import requests
from typing import Optional, Dict, Any


class CountryMapper:
    COUNTRY_MAPPING = {
        'US': 'United States',
        'DE': 'Germany',
        'FR': 'France',
        'CA': 'Canada',
        'GB': 'United Kingdom',
        'NL': 'Netherlands',
        'JP': 'Japan',
        'SG': 'Singapore',
        'RU': 'Russia',
        'FI': 'Finland',
        'CH': 'Switzerland',
        'CN': 'China',
        'KR': 'South Korea',
        'AU': 'Australia',
        'HK': 'Hong Kong',
        'UA': 'Ukraine',
        'IT': 'Italy',
        'SE': 'Sweden',
        'BR': 'Brazil',
        'ES': 'Spain',
        'VN': 'Vietnam',
        'IN': 'India',
        'TW': 'Taiwan',
        'PL': 'Poland',
        'IE': 'Ireland',
        'ZA': 'South Africa',
        'NZ': 'New Zealand',
        'CZ': 'Czechia',
        'LT': 'Lithuania',
        'ID': 'Indonesia',
        'NO': 'Norway',
        'BE': 'Belgium',
        'IR': 'Iran',
        'AT': 'Austria',
        'EE': 'Estonia',
        'TR': 'Turkey',
        'MY': 'Myanmar',
        'HU': 'Hungary',
        'MX': 'Mexico',
        'TH': 'Thailand',
        'SI': 'Slovenia',
        'IL': 'Israel',
        'RO': 'Romania',
        'NG': 'Nigeria',
        'PT': 'Portugal',
        'SK': 'Slovakia',
        'RS': 'Serbia',
        'BG': 'Bulgaria',
        'LV': 'Latvia',
    }

    @staticmethod
    def get_country_name(code):
        """Convert country code to full country name."""
        return CountryMapper.COUNTRY_MAPPING.get(code, code)


class PathManager:
    ROOT_DIR = Path(__file__).parent.parent.parent
    DATA_DIR = ROOT_DIR / "data" / "processed" / "blockchains"
    PLOTS_DIR = ROOT_DIR / "plots" / "blockchains"

    @staticmethod
    def get_blockchain_name():
        """Get blockchain name from current script path."""
        import inspect

        frame = inspect.stack()[2]
        caller_path = Path(frame.filename)

        if 'blockchains' in caller_path.parts:
            blockchain = caller_path.stem  # Gets filename without extension
            return blockchain

    @staticmethod
    def get_paths(blockchain=None):
        """Get standard paths for a blockchain."""
        if blockchain is None:
            blockchain = PathManager.get_blockchain_name()

        # Create necessary directories
        data_dir = PathManager.DATA_DIR / blockchain
        plots_dir = PathManager.PLOTS_DIR / blockchain

        data_dir.mkdir(parents=True, exist_ok=True)
        plots_dir.mkdir(parents=True, exist_ok=True)

        return {
            'data': {
                'geographic': data_dir / "geographic_distribution.csv",
                'client': data_dir / "client_distribution.csv",
                'hosting': data_dir / "hosting_distribution.csv"
            },
            'plots': {
                'geographic': plots_dir / "geographic_distribution.png",
                'client': plots_dir / "client_distribution.png",
                'hosting': plots_dir / "hosting_distribution.png"
            }
        }


class DataProcessor:
    @staticmethod
    def save_processed_data(df, output_path):
        """Save processed data to CSV."""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)

    @staticmethod
    def extract_client_name(version_string):
        """Extract standardized client name from version string."""
        try:
            version = version_string.strip('/')
            client = version.split(':')[0]
            return client
        except:
            return 'Unknown'


class APIUtils:
    @staticmethod
    def make_request(url: str, method: str = "GET", **kwargs) -> Optional[Dict[str, Any]]:
        """
        Generic method to make API requests.

        Args:
            url: API endpoint
            method: HTTP method (GET/POST)
            **kwargs: Additional request parameters (headers, json, etc.)

        Returns:
            Optional[Dict]: JSON response data or None if request fails
        """
        try:
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"API request error: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error: {e}")
            return None