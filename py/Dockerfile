# One image for all three Python services; docker-compose picks the module to run.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY pyproject.toml ./
COPY dmean ./dmean
RUN pip install --no-cache-dir .

CMD ["python", "-m", "dmean.coordinator"]
