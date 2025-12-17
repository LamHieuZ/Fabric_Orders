FROM apache/spark:3.5.0

USER root

# 1) Add spark user
RUN useradd -ms /bin/bash spark || true

RUN apt-get update && apt-get install -y \
    python3 python3-pip \
    python3-dev \
    build-essential \
    libomp-dev

# 2) Install Python + pip libs for ETL
RUN apt-get update && apt-get install -y python3 python3-pip && \
    pip3 install --no-cache-dir \
      minio \
      pandas \
      pyarrow \
      requests \
      scikit-learn \
      matplotlib \
      seaborn \
      hdbscan==0.8.33 


# 3) Install Java & Hadoop AWS S3A jars (MinIO support)
RUN apt-get install -y openjdk-17-jdk curl wget && \
    mkdir -p /opt/spark/jars && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -P /opt/spark/jars/ && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.534/aws-java-sdk-bundle-1.12.534.jar -P /opt/spark/jars/


# ================================================================
# ⭐ 4) Add Iceberg runtime for Spark 3.5 (CHỈ 1 FILE NÀY LÀ ĐỦ)
# ================================================================
RUN wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
    -P /opt/spark/jars/



# Fix permissions
RUN chmod -R 755 /opt/spark


# Python PATH
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYTHONPATH="/usr/local/lib/python3.10/dist-packages:${PYTHONPATH}"

# Spark extra classpath
ENV SPARK_EXTRA_CLASSPATH=/opt/spark/jars/*
ENV CLASSPATH=/opt/spark/jars/*:${CLASSPATH}

USER spark
