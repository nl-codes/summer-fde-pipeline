import logging

from psycopg2.extras import Json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class JSONExtractor:
    def __init__(self, db_connector):
        self.db_connector = db_connector

    def load_to_landing(self, table_name, json_data):
        table_name = table_name.lower()
        conn = self.db_connector.get_connection()
        cursor = None
        try:
            cursor = conn.cursor()
            insert_stmt = f"""
                INSERT INTO landing.{table_name} (raw_data)
                VALUES (%s)
            """
            # Insert data
            cursor.execute(insert_stmt, (Json(json_data),))
            conn.commit()
            logger.info(f"Loaded JSON data to {table_name}")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error loading to {table_name}: {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()
            conn.close()
