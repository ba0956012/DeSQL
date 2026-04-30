FROM python:3.11-slim
WORKDIR /app
COPY chart_service/requirements.txt /app/chart_service/requirements.txt
RUN pip install --no-cache-dir -r chart_service/requirements.txt
COPY chart_service/ /app/chart_service/
EXPOSE 8081
CMD ["uvicorn", "chart_service.main:app", "--host", "0.0.0.0", "--port", "8081"]
