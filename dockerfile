FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

# Install Postgres + required tools
RUN apt-get update && \
    apt-get install -y postgresql postgresql-contrib gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY . .

# Expose Postgres port (internal only)
EXPOSE 5432

# Start Postgres then run app
CMD service postgresql start && python app.py
