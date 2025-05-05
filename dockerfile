FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

WORKDIR /app/cf_data_pipeline
RUN ln -sf /usr/local/bin/python3 /usr/local/bin/python

CMD ["python", "run_pipeline.py"]
