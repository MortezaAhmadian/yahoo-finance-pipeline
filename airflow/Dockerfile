FROM apache/airflow:2.7.3-python3.8

USER root

# Install system dependencies
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         gcc \
         build-essential \
         default-jdk \
         libpq-dev \
         netcat \
         curl \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Install Docker CLI for DockerOperator
RUN curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-20.10.9.tgz | \
    tar -xzC /tmp && \
    mv /tmp/docker/docker /usr/local/bin/ && \
    chmod +x /usr/local/bin/docker && \
    rm -rf /tmp/docker

# Create directories for spark jobs - using the proper user ID instead of name
# The airflow user in the official image has UID 50000
RUN mkdir -p /opt/spark-apps && chown -R 50000:0 /opt/spark-apps

RUN groupadd -g 1003 docker_host \
    && usermod -aG docker_host airflow

# Add custom entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER airflow

# Install Python packages with constraints to ensure compatibility
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --user \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.3/constraints-3.8.txt" \
    -r /tmp/requirements.txt
