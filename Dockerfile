FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    unzip \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws

WORKDIR /app

COPY ./* ./

RUN pip install --no-cache-dir -r requirements.txt

COPY .aws /root/.aws

CMD ["python", "main.py"]
