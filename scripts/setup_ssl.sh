#!/bin/bash
set -euo pipefail

SSL_ARGS=""

if [ "${USE_SSL:-false}" = "true" ]; then
    KEYSTORE="/opt/ssl/keystore.jks"
    CERT="/opt/ssl/spark.crt"
    STOREPASS="${SSL_KEYSTORE_PASSWORD:-changeit}"

    # Generate keystore if it doesn't exist (e.g. not mounted as a volume)
    if [ ! -f "$KEYSTORE" ]; then
        echo "Generating self-signed certificate..."
        keytool -genkeypair \
            -alias spark \
            -keyalg RSA \
            -keysize 2048 \
            -validity 365 \
            -keystore "$KEYSTORE" \
            -storepass "$STOREPASS" \
            -keypass "$STOREPASS" \
            -dname "CN=localhost, OU=dev, O=dev, L=dev, S=dev, C=US" \
            -noprompt
    fi

    # Export the cert for clients to trust (idempotent)
    if [ ! -f "$CERT" ]; then
        keytool -exportcert \
            -alias spark \
            -keystore "$KEYSTORE" \
            -storepass "$STOREPASS" \
            -rfc \
            -file "$CERT"
        echo "Certificate exported to $CERT"
    fi

    SSL_ARGS="
        --conf spark.ssl.enabled=true
        --conf spark.ssl.keyStore=${KEYSTORE}
        --conf spark.ssl.keyStorePassword=${STOREPASS}
        --conf spark.ssl.keyPassword=${STOREPASS}
        --conf spark.ssl.protocol=TLSv1.2
    "
fi

export SSL_ARGS
export SSL_CERT="${CERT:-}"
export SSL_KEYSTORE="${KEYSTORE:-}"