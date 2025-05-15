# Use Python 3.12 as the base image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY pyproject.toml .
COPY README.md .
COPY runtime_config.json .
COPY src/ src/

# Install project dependencies
RUN pip install --no-cache-dir .

# Set environment variables
ENV PYTHONPATH=/app

# Create a directory for logs
RUN mkdir -p /app/logs

# Set the entrypoint to handle all arguments
ENTRYPOINT ["mcp_snowflake_server"]

