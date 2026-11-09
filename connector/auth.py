import os

from dotenv import load_dotenv
from databricks import sql


load_dotenv()


def get_connection():
    server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    access_token = os.getenv("DATABRICKS_TOKEN")

    if not server_hostname:
        raise RuntimeError("DATABRICKS_SERVER_HOSTNAME is not configured")

    if not http_path:
        raise RuntimeError("DATABRICKS_HTTP_PATH is not configured")

    if not access_token:
        raise RuntimeError("DATABRICKS_TOKEN is not configured")

    return sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token,
        session_configuration={"timezone": "UTC"},
    )


def test_connection():
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()

    return result


if __name__ == "__main__":
    print("Testing Databricks connection...")

    result = test_connection()

    print("Authentication: successful")
    print(f"Test query result: {result}")
