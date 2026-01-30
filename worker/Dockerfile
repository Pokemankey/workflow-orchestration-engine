FROM python:3.10-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY worker/requirements.txt /app/worker/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /app/worker/requirements.txt

# Copy shared module
COPY shared /app/shared

# Copy worker code
COPY worker /app/worker

# Run the application
CMD ["python", "-m", "worker.main"]