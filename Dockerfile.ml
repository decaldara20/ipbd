FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir typing-extensions>=4.10.0 sympy fsspec

RUN pip install --no-cache-dir torch==2.6.0+cpu --extra-index-url https://download.pytorch.org/whl/cpu

RUN pip install --no-cache-dir transformers pandas scikit-learn minio datasets safetensors accelerate

RUN pip install --no-cache-dir transformers pandas scikit-learn minio datasets safetensors accelerate psycopg2-binary