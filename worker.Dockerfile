FROM python:3.11-slim
WORKDIR /worker
RUN pip install --no-cache-dir 'psycopg[binary]==3.2.10' 'prometheus-client==0.22.1'
COPY worker.py slice_processing.py ./
ENV PYTHONUNBUFFERED=1
USER 10001
ENTRYPOINT ["python", "worker.py"]
