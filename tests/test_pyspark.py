import docker
import pytest
from pyspark.sql import SparkSession
import os
import time
import uuid


def wait_for_log(container, message, timeout=30):
    start = time.time()
    while True:
        logs = container.logs().decode("utf-8")
        if message in logs:
            return
        if time.time() - start > timeout:
            raise TimeoutError(f"Message '{message}' not found in logs")
        time.sleep(0.5)


@pytest.fixture(scope="function")
def db_backend_url():
    docker_client = docker.from_env()

    catalog = os.getenv("CATALOG")
    catalog_version = os.getenv("CATALOG_VERSION")
    spark_version = os.getenv("SPARK_VERSION")
    scala_version = os.getenv("SCALA_VERSION")
    java_version = os.getenv("JAVA_VERSION")

    tag = f"{catalog}{catalog_version}-spark{spark_version}-java{java_version}-scala{scala_version}"

    container = docker_client.containers.run(
        image=f"franciscoabsampaio/spark-connect-server:{tag}",
        detach=True,
        ports={'15002/tcp': 15002}
    )
    wait_for_log(container, message="Spark Connect server started")
    conn_url = "sc://localhost:15002"

    yield conn_url

    container.stop()


def qualified_name(catalog, schema, table):
    if catalog == "spark_catalog":
        return table
    return f"{catalog}.{schema}.{table}"


def test_catalog_basic_write_read(db_backend_url):
    spark = SparkSession.builder \
        .remote(db_backend_url) \
        .getOrCreate()

    catalog = os.getenv("CATALOG", "spark_catalog")
    schema = "default"
    table_name = f"test_{uuid.uuid4().hex[:8]}"
    
    full_name = qualified_name(catalog, schema, table_name)

    # CREATE TABLE USING CATALOG
    spark.sql(f"""
        CREATE TABLE {full_name} (
            id INT,
            name STRING
        )
        USING {os.getenv('CATALOG')}
    """)

    spark.sql(f"INSERT INTO {table_name} VALUES (1, 'bird'), (2, 'spark')")

    df = spark.sql(f"SELECT * FROM {table_name} ORDER BY id")
    rows = df.collect()

    assert len(rows) == 2
    assert rows[0]["id"] == 1
    assert rows[0]["name"] == "bird"

    spark.sql(f"DROP TABLE {table_name}")
    spark.stop()
