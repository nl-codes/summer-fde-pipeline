import psycopg2
from sqlalchemy import create_engine
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseConnector:
    def __init__(self, config):
        self.config = config['database']

    def get_connection(self):
        return psycopg2.connect(**self.config)

    def get_engine(self):
        conn_str = f"postgresql://{self.config['user']}:{self.config['password']}@{self.config['host']}:{self.config['port']}/{self.config['database']}"
        return create_engine(conn_str)
