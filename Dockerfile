# === Build stage (installs deps) ===
FROM python:3.11-slim AS base
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# System deps (psycopg2/pyarrow often need these)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl git && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# === Runtime stage ===
FROM python:3.11-slim
WORKDIR /app
COPY --from=base /usr/local /usr/local
COPY . .

# Default: run Streamlit app
EXPOSE 8501
CMD ["bash", "-lc", "streamlit run streamlit_app/app.py --server.port=8501 --server.address=0.0.0.0"]
