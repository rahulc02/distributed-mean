# One image for all three Python services; docker-compose picks the module to run.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DEFAULT_TIMEOUT=60 PIP_RETRIES=10
WORKDIR /app

# Dependencies first, in their own layer, so code changes do not re-download them.
COPY pyproject.toml ./
RUN mkdir dmean && touch dmean/__init__.py && pip install --no-cache-dir . && rm -rf dmean

COPY dmean ./dmean
RUN pip install --no-cache-dir --no-deps .

CMD ["python", "-m", "dmean.coordinator"]
