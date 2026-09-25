FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY database ./database
COPY health ./health
COPY integrity ./integrity
COPY metadata ./metadata
COPY rebalance ./rebalance
COPY repair ./repair
COPY replication ./replication
COPY storage ./storage
COPY worker ./worker

CMD ["python", "-m", "worker.main"]
