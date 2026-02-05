FROM python:3.11-slim

# -----------------------------
# Runtime environment
# -----------------------------
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# -----------------------------
# Install system deps (minimal)
# -----------------------------
# psycopg2-binary does NOT require build deps
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# -----------------------------
# Copy packaging metadata FIRST
# (best Docker layer caching)
# -----------------------------
COPY pyproject.toml ./

# If you later add setup.py / setup.cfg, include them here:
# COPY pyproject.toml setup.py setup.cfg ./

# -----------------------------
# Install app + deps
# -----------------------------
RUN pip install --upgrade pip \
 && pip install .

# -----------------------------
# Copy application code
# -----------------------------
COPY . .

# -----------------------------
# Start Gunicorn
# Railway provides $PORT
# -----------------------------
CMD ["sh", "-c", "exec gunicorn 'run:app' -b 0.0.0.0:${PORT:-8080} --workers ${WEB_CONCURRENCY:-2} --threads ${GTHREADS:-4} --timeout ${TIMEOUT:-120} --access-logfile - --error-logfile -"]
