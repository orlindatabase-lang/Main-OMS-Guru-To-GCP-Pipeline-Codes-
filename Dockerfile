FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY oms_daily_sync.py .
ENTRYPOINT ["python", "oms_daily_sync.py"]
