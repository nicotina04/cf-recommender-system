FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN ln -sf /usr/local/bin/python3 /usr/local/bin/python

CMD ["python", "cf_data_pipeline/run_pipeline.py"]
