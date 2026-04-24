FROM python:3.11-slim

WORKDIR /app

# Keep Python output unbuffered for real-time logs in Docker orchestrators.
ENV PYTHONUNBUFFERED=1

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY config.py .
COPY companycam_service.py .
COPY marketsharp_service.py .
COPY webhook_handler.py .
COPY security.py .
COPY app.py .

# Expose the default app port (can be overridden with FLASK_PORT).
EXPOSE 5001

# Persist dedupe DB if desired: mount /data and set IDEMPOTENCY_DB_PATH=/data/cc_webhook_dedupe.db

# Run the application
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:5001", "app:app"]
