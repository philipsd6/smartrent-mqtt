FROM python:3.13-slim

# prevent Python from writing .pyc files and enable unbuffered stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Create app user
RUN groupadd -r app && useradd -r -g app app
WORKDIR /app

# Copy and install requirements
COPY requirements.txt /app/requirements.txt
RUN pip install --root-user-action=ignore --upgrade pip
RUN pip install --root-user-action=ignore --no-cache-dir -r /app/requirements.txt

# Copy app
COPY smartrent-mqtt.py /app/smartrent-mqtt.py

# Use non-root user
USER app

# Expose health port
EXPOSE 8000

ENV SMARTRENT_EMAIL=""
ENV SMARTRENT_PASSWORD=""
ENV SMARTRENT_OTPAUTH=""
ENV MQTT_BROKER="localhost"
ENV MQTT_PORT=1883

ENV HEALTH_BIND_ADDRESS="0.0.0.0"
ENV HEALTH_PORT=8000
ENV LOG_LEVEL="INFO"

CMD ["python", "smartrent-mqtt.py"]
