test:
	docker build -f base.Dockerfile \
		--build-arg CATALOG_VERSION=3.3.2 \
		--build-arg JAVA_VERSION=17 \
		--build-arg SCALA_VERSION=2.12 \
		--build-arg SPARK_VERSION=3.5.7 \
		-t spark-base .

	docker build -f delta.Dockerfile \
		-t delta-local .

	. venv/bin/activate && SPARK_CONNECT_SERVER_LOCAL=true pytest -vs