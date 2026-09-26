FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

RUN useradd --create-home --uid 10001 vault     && mkdir -p /data/vault /var/lib/vault     && chown -R vault:vault /data /var/lib/vault

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN chown -R vault:vault /app

USER vault

EXPOSE 9001
CMD ["uvicorn", "storage.node_server:app", "--host", "0.0.0.0", "--port", "9001"]
