# Spark Connect Server

![The Docker whale brings the fun!](./docs/docker_whale.webp)

This repository contains the necessary Dockerfiles and supplementary code for building images of **Spark Connect servers**, with optional support for **Delta Lake** and **Apache Iceberg** catalogs.

> **Note:** These images are intended for **development and testing** purposes — not production use.

They were originally created to support the [`spark-connect`](https://github.com/franciscoabsampaio/spark-connect) (Rust client for Spark Connect) and [`Swellow`](https://github.com/franciscoabsampaio/swellow) projects, in an effort to achieve easy, **reproducible Spark environments** for integration testing, CI pipelines, and local development.

If you find these useful and wish to make them production-ready, feel free to open a discussion or reach out!

---

## 🧭 Getting Started

Run a Spark Connect server container:

```bash
docker run -P franciscoabsampaio/spark-connect-server:delta
```

The `-P` flag automatically exposes all configured ports (including the Spark UI and Spark Connect server) on higher-level TCP ports.

Once running, connect to it from PySpark or any Spark Connect client:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

df = spark.sql("SELECT 1 AS id")

print(df.collect())
```

### Running with SSL

Containers can also be run with SSL enabled, by setting the environment variable `USE_SSL=true` and either mounting the certificate and key store on the container:

```bash
docker run \
    -e USE_SSL=true \
    -v $PATH_TO_MY_KEYSTORE="/opt/spark/conf/ssl/keystore.jks":ro \
    -v $PATH_TO_MY_CERT="/opt/spark/conf/ssl/spark.crt":ro \
    -P franciscoabsampaio/spark-connect-server:delta
```

or letting the container generate a self-signed certificate, accessible through the volume (ensure the container can read and write to the directory):

```bash
docker run \
    -e USE_SSL=true \
    -v $PATH_TO_MY_CERT="/opt/spark/conf/ssl/spark.crt" \
    -P franciscoabsampaio/spark-connect-server:delta
```

Then specify the certificate when instantiating the Spark session:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .remote("sc://localhost:15002") \
    .config("spark.connect.grpc.ssl.enabled", "true") \
    .config("spark.connect.grpc.ssl.trustCertCollectionFile", path_to_my_cert) \
    .getOrCreate()

df = spark.sql("SELECT 1 AS id")

print(df.collect())
```

---

## 🏷️ Tags & Catalogs

Available image tags:

| Tag              | Description                              | Default Catalog              | Notes                        |
| ---------------- | ---------------------------------------- | ---------------------------- | ---------------------------- |
| `delta`          | Spark Connect server with Delta Lake     | Delta 3.3.2 / Spark 3.5.7    | Compatible with Java 17 |
| `delta-latest`   | Latest tested Delta build                | Delta 4.0.0 / Spark 4.0.1    | Uses Java 21 + Scala 2.13    |
| `iceberg`        | Spark Connect server with Apache Iceberg | Iceberg 1.6.1 / Spark 3.5.7  | Compatible with Java 17      |
| `iceberg-latest` | Latest tested Iceberg build              | Iceberg 1.10.1 / Spark 4.0.1 | Uses Java 21 + Scala 2.13    |

Each tag corresponds to a prebuilt environment combination of:

* **Spark version**
* **Scala version**
* **Java version**
* **Catalog (Delta / Iceberg)**
* **Catalog version**

These images ensure the correct set of JARs and environment variables are configured automatically, minimizing setup friction.

> **Tip:** If you need to target a specific Spark or Java version, use one of the `Dockerfile`'s in the repository as a template for a custom build.

---

## 🔗 Useful Links

* [Official Spark Docker image tags](https://hub.docker.com/_/spark/tags)
* [Apache Spark releases](https://spark.apache.org/docs/)
* [Delta Lake releases](https://delta-docs-incubator.netlify.app/releases/)
* [delta-spark artifacts](https://mvnrepository.com/artifact/io.delta/delta-spark)
* [delta-storage artifacts](https://mvnrepository.com/artifact/io.delta/delta-storage)
* [Iceberg runtime (Spark 3.5)](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-spark-runtime-3.5)

---

© 2025 Francisco A. B. Sampaio. Licensed under the MIT License.

This project is not affiliated with, endorsed by, or sponsored by the Apache Software Foundation.
