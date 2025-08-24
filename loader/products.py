from psycopg2 import sql
from loader.utils import get_db_connection, execute_query, get_schemas, get_entities

ENTITY = 'products'

def load_products():
    """Load products data from staging to target"""
    schemas = get_schemas()
    entities = get_entities()

    conn = get_db_connection()
    try:
        # Step 1: Clear temp table
        execute_query(conn, f"TRUNCATE TABLE {schemas['transform_schema']}.{entities[ENTITY]['temp_table']}")

        # Step 2: Load staging data to temp table
        load_query = sql.SQL("""
            INSERT INTO {transform_table} (
                product_id, product_name, category, brand, price,
                stock_quantity, sku, source_system, source_loaded_at
            )
            SELECT
                product_id, product_name, category, brand, price,
                stock_quantity, sku, source_system, source_loaded_at
            FROM {staging_view}
        """).format(
            transform_table=sql.Identifier(schemas['transform_schema'], entities[ENTITY]['temp_table']),
            staging_view=sql.Identifier(schemas['staging_schema'], entities[ENTITY]['staging_view'])
        )
        execute_query(conn, load_query)

        # Step 3: Merge into target table
        merge_query = sql.SQL("""
            INSERT INTO {target_table} (
                product_id, product_name, category, brand, price,
                stock_quantity, sku, source_system
            )
            SELECT
                product_id, product_name, category, brand, price,
                stock_quantity, sku, source_system
            FROM {transform_table}
            ON CONFLICT (product_id) DO UPDATE SET
                product_name = EXCLUDED.product_name,
                category = EXCLUDED.category,
                brand = EXCLUDED.brand,
                price = EXCLUDED.price,
                stock_quantity = EXCLUDED.stock_quantity,
                sku = EXCLUDED.sku,
                source_system = EXCLUDED.source_system,
                updated_at = CURRENT_TIMESTAMP
        """).format(
            target_table=sql.Identifier(schemas['target_schema'], entities[ENTITY]['target_table']),
            transform_table=sql.Identifier(schemas['transform_schema'], entities[ENTITY]['temp_table'])
        )
        execute_query(conn, merge_query)

        print("Products loaded successfully")

    finally:
        conn.close()

if __name__ == "__main__":
    load_products()
