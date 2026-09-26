FROM python:3.12-alpine

WORKDIR /srv

RUN addgroup -S -g 10001 vault && adduser -S -u 10001 -G vault vault

COPY --chown=vault:vault frontend ./frontend

USER vault

EXPOSE 5173
CMD ["python", "-m", "http.server", "5173", "--bind", "0.0.0.0", "--directory", "/srv/frontend"]
