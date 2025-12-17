
FROM hyperledger/fabric-tools:2.5

WORKDIR /app

# Cài pip trước
RUN apt-get update && \
    apt-get install -y python3-pip && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY listener.py .

CMD ["python3", "/app/listener.py"]
