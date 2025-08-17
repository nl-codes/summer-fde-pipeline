from psycopg2 import sql
from utils import get_db_connection, execute_query, get_schemas, get_entities

ENTITY = 'sales'

def load_sales():
    """Load sales data from staging to target"""
    schemas = get_schemas()
    entities = get_entities()

    conn = get_db_connection()
    try:
        # Step 1: Clear temp table
        execute_query(conn, f"TRUNCATE TABLE {schemas['transform_schema']}.{entities[ENTITY]['temp_table']}")

        # Step 2: Load staging data to temp table
        load_query = sql.SQL("""
            INSERT INTO {transform_table} (
                sale_id, sale_date, store_id, product_id, customer_id,
                quantity, unit_price, total_amount, source_system, source_loaded_at
            )
            SELECT
                sale_id, sale_date, store_id, product_id, customer_id,
                quantity, unit_price, total_amount, source_system, source_loaded_at
            FROM {staging_view}
        """).format(
            transform_table=sql.Identifier(schemas['transform_schema'], entities[ENTITY]['temp_table']),
            staging_view=sql.Identifier(schemas['staging_schema'], entities[ENTITY]['staging_view'])
        )
        execute_query(conn, load_query)

        # Step 3: Fill missing unit_price from product dimension
        fill_price_query = sql.SQL("""
            UPDATE {transform_table} t
            SET unit_price = p.price
            FROM {product_table} p
            WHERE t.unit_price IS NULL
              AND t.product_id = p.product_id;
        """).format(
            transform_table=sql.Identifier(schemas['transform_schema'], entities[ENTITY]['temp_table']),
            product_table=sql.Identifier(schemas['target_schema'], 'dim_products')
        )
        execute_query(conn, fill_price_query)

        # Calculate missing total_amount
        calculate_query = sql.SQL("""
            UPDATE {transform_table}
            SET total_amount = quantity * unit_price
            WHERE total_amount IS NULL
            AND unit_price IS NOT NULL
            AND quantity IS NOT NULL;
        """).format(
            transform_table=sql.Identifier(schemas['transform_schema'], entities[ENTITY]['temp_table'])
        )
        execute_query(conn, calculate_query)

       # Step 4: Insert into target table using composite key with upsert, deduplicated
        insert_query = sql.SQL("""
          INSERT INTO {target_table} (
              sale_id, sale_date, store_id, product_id, customer_id,
              quantity, unit_price, total_amount, source_system
          )
          SELECT DISTINCT ON (sale_id, product_id)
              sale_id, sale_date, store_id, product_id, customer_id,
              quantity, unit_price, total_amount, source_system
          FROM {transform_table}
          ORDER BY sale_id, product_id, source_loaded_at DESC
          ON CONFLICT (sale_id, product_id) DO NOTHING
          """).format(
          target_table=sql.Identifier(schemas['target_schema'], entities[ENTITY]['target_table']),
          transform_table=sql.Identifier(schemas['transform_schema'], entities[ENTITY]['temp_table'])
          )

        execute_query(conn, insert_query)

        print("Sales loaded successfully")

    except Exception as e:
        print(f"Error loading sales: {str(e)}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    load_sales()
