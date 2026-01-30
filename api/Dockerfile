FROM python:3.10-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY api/requirements.txt /app/api/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /app/api/requirements.txt

# Copy shared module
COPY shared /app/shared

# Copy orchestrator (needed for DAG validator import)
COPY orchestrator /app/orchestrator

# Copy API code
COPY api /app/api

# Expose port
EXPOSE 8000

# Run the application
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]