FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

# Install PostgreSQL client dependencies
RUN apt-get update && apt-get install -y gcc libpq-dev curl && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy ETL code
COPY . .

# Default command: run the ETL script
CMD ["python", "app.py"]
