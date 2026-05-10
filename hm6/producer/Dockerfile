FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends curl gcc && rm -rf /var/lib/apt/lists/*
COPY producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY producer/ /app/
COPY schemas/  /app/schemas/
CMD ["python", "-u", "main.py"]
