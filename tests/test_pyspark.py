import docker
import pytest
from pyspark.sql import SparkSession
import os
import time
import uuid


def wait_for_log(container, message, timeout=30):
    start = time.time()
    while True:
        # Refresh the container object from the Docker daemon
        container.reload()
        logs = container.logs().decode("utf-8")
        
        # Check if the container is still running
        if container.status == "exited":
            raise RuntimeError(f"Container crashed prematurely!\nContainer Logs:\n{logs}")

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
def image_name():
    if os.getenv("SPARK_CONNECT_SERVER_LOCAL"):
        return "delta-local"
    
    catalog = os.getenv("CATALOG", "delta")
    catalog_version = os.getenv("CATALOG_VERSION")
    spark_version = os.getenv("SPARK_VERSION")
    scala_version = os.getenv("SCALA_VERSION")
    java_version = os.getenv("JAVA_VERSION")

    tag = f"{catalog}{catalog_version}-spark{spark_version}-java{java_version}-scala{scala_version}"
    return f"franciscoabsampaio/spark-connect-server:{tag}"


@pytest.fixture(scope="function")
def db_backend_url(image_name):
    docker_client = docker.from_env()

    container = docker_client.containers.run(
        image=image_name,
        detach=True,
        ports={'15002/tcp': 15002}
    )
    wait_for_log(container, message="Spark Connect server started")
    conn_url = "sc://localhost:15002"

    yield conn_url

    container.stop()


def test_catalog_basic_write_read(db_backend_url):
    spark = SparkSession.builder \
        .remote(db_backend_url) \
        .getOrCreate()

    catalog = os.getenv("CATALOG", "delta")
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
def db_backend_url_ssl(image_name, tmp_path):
    docker_client = docker.from_env()

    ssl_dir = tmp_path / "ssl"
    ssl_dir.mkdir()
    # Make the host folder writable by any user inside the container
    ssl_dir.chmod(0o777)

    container = docker_client.containers.run(
        image=image_name,
        detach=True,
        ports={'15002/tcp': 15002},
        environment={"USE_SSL": "true"},
        volumes={str(ssl_dir): {"bind": "/opt/conf/ssl", "mode": "rw"}},
    )

    wait_for_log(container, message="Spark Connect server started")

    # Wait for the cert to be written to the mounted volume
    cert_path = ssl_dir / "spark.crt"
    wait_for_cert(cert_path)

    # from urllib.parse import quote
    # encoded_cert_path = quote(str(cert_path))
    # "ssl_trustCertCollectionFile={encoded_cert_path}"
    yield cert_path, f"sc://localhost:15002"

    container.stop()


def test_catalog_basic_write_read_ssl(db_backend_url_ssl):
    cert_path, connection_url = db_backend_url_ssl
    
    spark = SparkSession.builder \
        .remote(connection_url) \
        .config("spark.connect.grpc.ssl.enabled", "true") \
        .config("spark.connect.grpc.ssl.trustCertCollectionFile", cert_path) \
        .getOrCreate()

    catalog = os.getenv("CATALOG", "delta")
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