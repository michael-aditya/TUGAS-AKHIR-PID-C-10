FROM python:3.11-slim

WORKDIR /app
COPY simulator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common /app/common
COPY simulator /app/simulator
ENV PYTHONPATH=/app

CMD ["python", "/app/common/wait_for.py", "mongodb:27017", "mosquitto:1883", "--", "python", "-m", "simulator.main"]
