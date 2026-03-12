FROM apache/airflow:3.1.6

USER airflow

WORKDIR /app .

COPY . .

RUN pip install --no-cache-dir -r requirements.txt
