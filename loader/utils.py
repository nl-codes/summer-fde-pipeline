import psycopg2
import yaml
import os
from string import Template
from dotenv import load_dotenv

load_dotenv()

def load_config():
    """Load configuration from YAML file"""
    config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
    with open(config_path, 'r') as file:
        config_content = file.read()

    template = Template(config_content)
    config_str = template.substitute(os.environ)
    return yaml.safe_load(config_str)

def get_db_connection():
    """Get database connection"""
    config = load_config()
    return psycopg2.connect(**config['database'])

def get_schemas():
    """Get schema configuration"""
    return load_config()['schemas']

def get_entities():
    """Get entities configuration"""
    return load_config()['entities']

def execute_query(conn, query, params=None):
    """Execute SQL query"""
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Database error: {str(e)}")
