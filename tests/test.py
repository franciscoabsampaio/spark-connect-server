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
def db_backend():
    docker_client = docker.from_env()

    catalog = os.getenv("CATALOG")
    catalog_version = os.getenv("CATALOG_VERSION")
    spark_version = os.getenv("SPARK_VERSION")
    scala_version = os.getenv("SCALA_VERSION")
    java_version = os.getenv("JAVA_VERSION")

    tag = f"{catalog}{catalog_version}-spark{spark_version}-java{java_version}-scala{scala_version}"

    image = docker_client.images.pull("franciscoabsampaio/spark-connect", tag=tag)
    container = docker_client.containers.run(
        image,
        detach=True,
        ports={'15002/tcp': 15002}
    )
    wait_for_log(container, message="Spark Connect server started at:")
    conn_url = "sc://localhost:15002"

    yield conn_url

    container.stop()


def test_delta_catalog_basic_write_read():
    spark = SparkSession.builder \
        .remote("sc://localhost:15002") \
        .getOrCreate()

    table_name = f"{os.getenv("CATALOG")}_test_{uuid.uuid4().hex[:8]}"
    location = f"/tmp/{table_name}"  # Works in local fs or container /tmp

    # CREATE TABLE USING CATALOG
    spark.sql(f"""
        CREATE TABLE {table_name} (
            id INT,
            name STRING
        )
        USING {os.getenv("CATALOG")}
        LOCATION '{location}'
    """)

    spark.sql(f"INSERT INTO {table_name} VALUES (1, 'bird'), (2, 'spark')")

    df = spark.sql(f"SELECT * FROM {table_name} ORDER BY id")
    rows = df.collect()

    assert len(rows) == 2
    assert rows[0]["id"] == 1
    assert rows[0]["name"] == "bird"

    spark.sql(f"DROP TABLE {table_name}")
    spark.stop()
