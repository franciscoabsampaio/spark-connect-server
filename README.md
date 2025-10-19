# Spark Connect Server

This repository contains the necessary Dockerfiles and supplementary code for building images of Spark Connect servers.

**It is not tested for production.**

*Instead*, It was created in the context of the [Spark Connect client for Rust](https://github.com/franciscoabsampaio/spark-connect) and [Swellow](https://github.com/franciscoabsampaio/swellow) projects to facilitate testing and development, in easy, reproducible fashion!

Feel free to use it. If you wish to productize it further, let me know!

## Getting Started

```sh
docker run -P franciscoabsampaio/spark-connect 
```

`-P` will expose all configured ports (Spark UI and Spark Connect Server) in higher-level TCP ports.

## Tags

- **Delta:** initializes the Spark Connect server with a Delta catalog.
- **Iceberg:** initializes the Spark Connect server with an Iceberg catalog.
