import logging

import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class APIExtractor:
    def __init__(self, config, json_extractor):
        self.endpoints_mapping = config["api"]["endpoints"]
        self.json_extractor = json_extractor

    def extract_endpoint(self, url, table_name):
        table_name = table_name.lower()
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            json_data = response.json()

            self.json_extractor.load_to_landing(table_name, json_data)

            logger.info(f"Loaded API data from {url} to {table_name}")

        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP error processing {url}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error processing {url}: {str(e)}")
            raise

    def extract_all(self):
        for url, table_name in self.endpoints_mapping.items():
            logger.info(f"Processing {url}")
            self.extract_endpoint(url, table_name)
