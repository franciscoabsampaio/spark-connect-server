# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

Images are not versioned independently: each tag names the catalog, Spark, Java
and Scala versions it was built from, so entries here describe changes to the
images themselves rather than to a release number.

## Unreleased

## 20-09-2026

### Added

- `HEALTHCHECK` in every image: the container reports healthy once Spark
  Connect accepts connections, so consumers can wait on `docker inspect`
  health, Compose's `service_healthy` or a readiness probe instead of scraping
  the startup log. The port is probed from inside the container, because a
  published port is served by Docker's proxy, which accepts connections before
  the server itself listens.
- `CHANGELOG.md`.

### Changed

- Tests wait on the container's health status, falling back to the startup log
  line for images built before the `HEALTHCHECK` was added.
