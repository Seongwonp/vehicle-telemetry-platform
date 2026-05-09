FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY vehicle_simulator.py .

CMD ["python", "vehicle_simulator.py"]
