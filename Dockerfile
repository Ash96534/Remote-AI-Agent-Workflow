# Dockerfile — Remote AI Agent Workflow
FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY server.py client.py config.ini ./

# Expose the FastAPI port
EXPOSE 8080

# Environment variables with sensible defaults
ENV AIPIPE_HOST=0.0.0.0
ENV AIPIPE_PORT=8080
ENV AIPIPE_START_TUNNEL=0

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

# Run the server
CMD ["python", "server.py"]
