FROM docker.io/airbyte/python-connector-base:2.0.0@sha256:c44839ba84406116e8ba68722a0f30e8f6e7056c726f447681bb9e9ece8bd916

WORKDIR /airbyte/integration_code

# Copy project files needed for build
COPY pyproject.toml poetry.lock README.md ./
COPY dist/*.whl ./dist/

# Install dependencies - ignore keyring warnings
RUN poetry config virtualenvs.create false \
    && poetry install --only main --no-interaction --no-ansi || true

# Copy source code
COPY airbyte_cdk ./airbyte_cdk

# Build and install the package
RUN pip install dist/*.whl

# Create main.py with the correct import
RUN echo 'from airbyte_cdk.cli.source_declarative_manifest._run import run\n\nif __name__ == "__main__":\n    run()' > /airbyte/integration_code/main.py

# Add debug step to verify file contents
RUN echo "Verifying main.py contents:" && cat /airbyte/integration_code/main.py

ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]
