FROM python:3.9-slim

WORKDIR /app

# Paksa instal dependensi yang bermasalah dari server resmi PyPI sebelum PyTorch
RUN pip install --no-cache-dir typing-extensions>=4.10.0 sympy fsspec

# Instal PyTorch menggunakan --extra-index-url agar PIP bisa mencari ke PyPI
RUN pip install --no-cache-dir torch==2.6.0+cpu --extra-index-url https://download.pytorch.org/whl/cpu

# PERBAIKAN FINAL: Menambahkan 'accelerate' untuk manajemen memori HuggingFace Trainer
RUN pip install --no-cache-dir transformers pandas scikit-learn minio datasets safetensors accelerate

RUN pip install --no-cache-dir transformers pandas scikit-learn minio datasets safetensors accelerate psycopg2-binary