import logging
import re
from datetime import datetime
from sqlalchemy import text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CSVExtractor:
    def __init__(self, db_connector):
        self.db_connector = db_connector

    @staticmethod
    def camel_to_snake(name):
        """
        Convert camelCase or PascalCase to snake_case.
        Example: 'customerKey' -> 'customer_key', 'StoreID' -> 'store_id'
        """
        s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
        return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()

    def get_table_columns(self, table_name, schema="landing"):
        """Get the actual column names from the database table"""
        table_name = table_name.lower()
        engine = self.db_connector.get_engine()
        try:
            with engine.connect() as conn:
                result = conn.execute(
                    text(
                        f"""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = '{schema}'
                        AND table_name = '{table_name}'
                        ORDER BY ordinal_position
                        """
                    )
                )
                columns = [row[0] for row in result]
                logger.info(f"Table {schema}.{table_name} has columns: {columns}")
                return columns
        except Exception as e:
            logger.error(f"Error getting table columns for {table_name}: {str(e)}")
            raise

    def load_to_landing(self, table_name, df):
        table_name = table_name.lower()
        table_columns = self.get_table_columns(table_name, schema="landing")
        engine = self.db_connector.get_engine()
        try:

            def normalize_column(col):
                col = col.replace(" ", "").replace("-", "_")
                col = re.sub(r"_+", "_", col)
                return self.camel_to_snake(col)

            df.columns = [normalize_column(col) for col in df.columns]

            # Only keep columns that exist in the table schema
            existing_columns = [col for col in df.columns if col in table_columns]
            missing_columns = [col for col in df.columns if col not in table_columns]
            if missing_columns:
                logger.warning(
                    f"The following columns will be skipped (not in table {table_name}): {missing_columns}"
                )

            df_filtered = df[existing_columns]
            df_filtered.to_sql(
                table_name, engine, schema="landing", if_exists="append", index=False
            )
            logger.info(
                f"Successfully loaded {len(df_filtered)} rows into {table_name}"
            )
        except Exception as e:
            logger.error(f"Error loading data into {table_name}: {e}")
            raise
