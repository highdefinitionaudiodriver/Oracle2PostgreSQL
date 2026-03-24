# ======================================================================
# Oracle → PostgreSQL Migration Tool - Docker Image
# Lightweight CLI-only container (no GUI/Tkinter)
# ======================================================================

FROM python:3.12-slim AS base

LABEL maintainer="Oracle2PostgreSQL Migration Tool"
LABEL description="Oracle to PostgreSQL SQL migration tool (CLI mode)"

# Prevent Python from writing .pyc files and enable unbuffered output
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install PostgreSQL client for optional result verification
RUN apt-get update && \
    apt-get install -y --no-install-recommends postgresql-client && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (cache-friendly layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY main.py .
COPY config.yaml .

# Create default directories
RUN mkdir -p /app/input /app/output /app/logs

# Default: run migration CLI
# Users override -i/-o via docker run or docker-compose
ENTRYPOINT ["python", "main.py"]
CMD ["--help"]

# ======================================================================
# Test stage - used for CI and local test runs
# ======================================================================
FROM base AS test

COPY test_conversion.py .
COPY samples/ ./samples/

CMD ["python", "-m", "pytest", "test_conversion.py", "-v", "--tb=short"]
