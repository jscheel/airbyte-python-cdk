# Builds as `airbyte/source-declarative-manifest`
# Usage:
#  docker build -t airbyte/source-declarative-manifest .
#  docker run airbyte/source-declarative-manifest --help
#  docker run airbyte/source-declarative-manifest spec
FROM docker.io/airbyte/python-connector-base:2.0.0@sha256:c44839ba84406116e8ba68722a0f30e8f6e7056c726f447681bb9e9ece8bd916

# Copy source code into image
COPY .

RUN poetry install --no-dev
ENTRYPOINT [ "poetry run source-declarative-manifest" ]
