FROM python:3.11-slim

WORKDIR /app

# Install dependencies first for better layer caching
COPY eval_service/requirements.txt /app/eval_service/requirements.txt
RUN pip install --no-cache-dir -r eval_service/requirements.txt

# Copy application code
COPY eval_service/ /app/eval_service/

# Copy evaluation data
COPY eval/dev.json /app/data/dev.json
COPY eval/databases/ /app/data/databases/

# Set default data directory
ENV DATA_DIR=/app/data

EXPOSE 8080

CMD ["uvicorn", "eval_service.main:app", "--host", "0.0.0.0", "--port", "8080"]
