FROM python:3.12-alpine

WORKDIR /srv

COPY frontend ./frontend

EXPOSE 5173
CMD ["python", "-m", "http.server", "5173", "--bind", "0.0.0.0", "--directory", "/srv/frontend"]
