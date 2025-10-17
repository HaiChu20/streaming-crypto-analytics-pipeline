FROM flink:latest

# Install Python and pip
USER root
RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-dev wget && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download Flink Kafka connector JAR (using the SQL connector which includes everything)
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar

# Create job directory first
RUN mkdir -p /opt/flink

# Copy files (now context is project root, so paths are relative to that)
COPY ./requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Copy job files and .env
COPY ../../flink-jobs/stream-processor.py /opt/flink/stream-processor.py
COPY .env /opt/flink

# Switch back to flink user
USER flink

