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


def wait_for_cert(cert_path, timeout=30):
    start = time.time()
    while not cert_path.exists():
        if time.time() - start > timeout:
            raise TimeoutError(f"SSL certificate was not found in path '{cert_path}'")
        time.sleep(0.5)


@pytest.fixture
def tag():
    catalog = os.getenv("CATALOG")
    catalog_version = os.getenv("CATALOG_VERSION")
    spark_version = os.getenv("SPARK_VERSION")
    scala_version = os.getenv("SCALA_VERSION")
    java_version = os.getenv("JAVA_VERSION")

    return f"{catalog}{catalog_version}-spark{spark_version}-java{java_version}-scala{scala_version}"


@pytest.fixture(scope="function")
def db_backend_url(tag):
    docker_client = docker.from_env()

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
    # if catalog == 'iceberg':
    #     return f"spark_catalog.{schema}.{table}"
    return f"{schema}.{table}"


def test_catalog_basic_write_read(db_backend_url):
    spark = SparkSession.builder \
        .remote(db_backend_url) \
        .getOrCreate()

    catalog = os.getenv("CATALOG")
    table_name = f"test_{uuid.uuid4().hex[:8]}"

    # CREATE TABLE USING CATALOG
    spark.sql(f"""
        CREATE TABLE {table_name} (
            id INT,
            name STRING
        )
        USING {catalog}
    """)

    spark.sql(f"INSERT INTO {table_name} VALUES (1, 'bird'), (2, 'spark')")

    df = spark.sql(f"SELECT * FROM {table_name} ORDER BY id")
    rows = df.collect()

    assert len(rows) == 2
    assert rows[0]["id"] == 1
    assert rows[0]["name"] == "bird"

    spark.sql(f"DROP TABLE {table_name}")
    spark.stop()


@pytest.fixture(scope="function")
def db_backend_url_ssl(tag, tmp_path):
    docker_client = docker.from_env()

    ssl_dir = tmp_path / "ssl"
    ssl_dir.mkdir()

    container = docker_client.containers.run(
        image=f"franciscoabsampaio/spark-connect-server:{tag}",
        detach=True,
        ports={'15002/tcp': 15002},
        environment={"USE_SSL": "true"},
        volumes={str(ssl_dir): {"bind": "/opt/spark/conf/ssl", "mode": "rw"}},
    )

    wait_for_log(container, message="Spark Connect server started")

    # Wait for the cert to be written to the mounted volume
    cert_path = ssl_dir / "spark.crt"
    wait_for_cert(cert_path)

    yield f"sc://localhost:15002;use_ssl=true;ssl_trustCertCollectionFile={cert_path}"

    container.stop()


def test_catalog_basic_write_read_ssl(db_backend_url_ssl):
    conn_url = db_backend_url_ssl

    spark = SparkSession.builder \
        .remote(conn_url) \
        .getOrCreate()

    catalog = os.getenv("CATALOG")
    table_name = f"test_{uuid.uuid4().hex[:8]}"

    spark.sql(f"""
        CREATE TABLE {table_name} (
            id INT,
            name STRING
        )
        USING {catalog}
    """)

    spark.sql(f"INSERT INTO {table_name} VALUES (1, 'bird'), (2, 'spark')")

    df = spark.sql(f"SELECT * FROM {table_name} ORDER BY id")
    rows = df.collect()

    assert len(rows) == 2
    assert rows[0]["id"] == 1
    assert rows[0]["name"] == "bird"

    spark.sql(f"DROP TABLE {table_name}")
    spark.stop()