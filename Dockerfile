FROM python:latest
WORKDIR /app
COPY . /app
RUN pip install -r /app/alpaca-demo/requirements.txt
RUN mkdir -p /app/alpaca-demo/logs
WORKDIR /app/alpaca-demo
CMD ["python", "/app/alpaca-demo/algo.py"]
# CMD ["python", "-m", "http.server"]
