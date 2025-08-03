import io
import json
import logging

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PublicS3Extractor:
    def __init__(self, config, json_extractor, csv_extractor):

        self.bucket_name = config["s3"]["bucket_name"]
        self.region = config["s3"].get("region", "us-east-1")
        self.files_mapping = config["s3"]["files"]
        self.json_extractor = json_extractor
        self.csv_extractor = csv_extractor

    def get_public_url(self, s3_key):
        # Construct the public S3 URL
        return f"https://{self.bucket_name}.s3.{self.region}.amazonaws.com/{s3_key}"

    def extract_file(self, s3_key, table_name):
        try:
            url = self.get_public_url(s3_key)
            logger.info(f"Fetching from URL: {url}")

            response = requests.get(url, timeout=30)
            response.raise_for_status()

            content = response.text

            if s3_key.endswith(".json"):
                data = json.loads(content)
                self.json_extractor.load_to_landing(table_name, data)
            elif s3_key.endswith(".csv"):
                df = pd.read_csv(io.StringIO(content))
                self.csv_extractor.load_to_landing(table_name, df)

            logger.info(f"Successfully processed {s3_key}")

        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP error processing {s3_key}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error processing {s3_key}: {str(e)}")
            raise

    def extract_all(self):
        for s3_key, table_name in self.files_mapping.items():
            logger.info(f"Processing {s3_key}")
            self.extract_file(s3_key, table_name)
