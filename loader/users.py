from psycopg2 import sql
from loader.utils import get_db_connection, execute_query, get_schemas, get_entities

ENTITY = 'users'

def load_users():
    """Load users data from staging to target"""
    schemas = get_schemas()
    entities = get_entities()

    conn = get_db_connection()
    try:
        # Step 1: Clear temp table
        execute_query(conn, f"TRUNCATE TABLE {schemas['transform_schema']}.{entities[ENTITY]['temp_table']}")

        # Step 2: Load staging data to temp table
        load_query = sql.SQL("""
            INSERT INTO {transform_table} (
                user_id, first_name, last_name, email, phone, age, gender,
                city, state, postal_code, country, source_system, source_loaded_at
            )
            SELECT
                user_id, first_name, last_name, email, phone, age, gender,
                city, state, postal_code, country, source_system, source_loaded_at
            FROM {staging_view}
        """).format(
            transform_table=sql.Identifier(schemas['transform_schema'], entities[ENTITY]['temp_table']),
            staging_view=sql.Identifier(schemas['staging_schema'], entities[ENTITY]['staging_view'])
        )
        execute_query(conn, load_query)

        # Step 3: Merge into target table
        merge_query = sql.SQL("""
            INSERT INTO {target_table} (
                user_id, first_name, last_name, email, phone, age, gender,
                city, state, postal_code, country, source_system
            )
            SELECT
                user_id, first_name, last_name, email, phone, age, gender,
                city, state, postal_code, country, source_system
            FROM {transform_table}
            ON CONFLICT (user_id) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                email = EXCLUDED.email,
                phone = EXCLUDED.phone,
                age = EXCLUDED.age,
                gender = EXCLUDED.gender,
                city = EXCLUDED.city,
                state = EXCLUDED.state,
                postal_code = EXCLUDED.postal_code,
                country = EXCLUDED.country,
                source_system = EXCLUDED.source_system,
                updated_at = CURRENT_TIMESTAMP
        """).format(
            target_table=sql.Identifier(schemas['target_schema'], entities[ENTITY]['target_table']),
            transform_table=sql.Identifier(schemas['transform_schema'], entities[ENTITY]['temp_table'])
        )
        execute_query(conn, merge_query)

        print("Users loaded successfully")

    finally:
        conn.close()

if __name__ == "__main__":
    load_users()
