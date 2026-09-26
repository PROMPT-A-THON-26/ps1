FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

RUN useradd --create-home --uid 10001 vault

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN chown -R vault:vault /app

USER vault

CMD ["celery", "-A", "worker.celery_app:celery_app", "worker", "--beat", "--loglevel=INFO"]
