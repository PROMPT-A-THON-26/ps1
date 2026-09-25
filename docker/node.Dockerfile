FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY storage ./storage

EXPOSE 9001

CMD ["uvicorn", "storage.node_server:app", "--host", "0.0.0.0", "--port", "9001"]
