FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY tdc/ ./tdc/
COPY configs/ ./configs/

ENV TDC_CONFIG_DIR=/app/configs
ENV TDC_LOG_LEVEL=INFO
ENV PYTHONPATH=/app

CMD ["python", "-m", "tdc.cli", "scheduler", "start"]
