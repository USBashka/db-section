from dotenv import load_dotenv
import os



load_dotenv()

DB_NAME = os.environ.get("DB_NAME")
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
