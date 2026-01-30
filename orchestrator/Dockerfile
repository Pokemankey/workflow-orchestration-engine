FROM python:3.10-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY orchestrator/requirements.txt /app/orchestrator/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /app/orchestrator/requirements.txt

# Copy shared module
COPY shared /app/shared

# Copy orchestrator code
COPY orchestrator /app/orchestrator

# Run the application
CMD ["python", "-m", "orchestrator.main"]