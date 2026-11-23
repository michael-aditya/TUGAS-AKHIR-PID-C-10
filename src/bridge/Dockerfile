FROM python:3.11-slim

WORKDIR /app
COPY bridge/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common /app/common
COPY bridge /app/bridge
ENV PYTHONPATH=/app

CMD ["python", "/app/common/wait_for.py", "kafka:29092", "mosquitto:1883", "mongodb:27017", "--", "python", "-m", "bridge.main"]
