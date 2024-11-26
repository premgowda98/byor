FROM python:3.11.8-slim-bullseye
WORKDIR /app
COPY app/main.py .
CMD ["python3", "main.py"]