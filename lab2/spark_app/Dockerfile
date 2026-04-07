FROM bde2020/spark-master:3.1.1-hadoop3.2

RUN apk update && apk add --no-cache \
    python3 \
    python3-dev \
    py3-pip \
    gcc \
    musl-dev \
    linux-headers

WORKDIR /opt/spark-app

COPY requirements.txt .

RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

#CMD ["/spark/bin/spark-submit", "--master", "spark://spark:7077", "/opt/spark-app/spark_app.py"]
