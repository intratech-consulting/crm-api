# Use the official Python LTS image
FROM python:3.12-slim

WORKDIR /app

# Install necessary packages for Salesforce CLI and supervisord
RUN apt-get update && \
    apt-get install -y wget xz-utils supervisor && \
    rm -rf /var/lib/apt/lists/*

# Download and install Salesforce CLI tarballs
RUN wget -qO sf-linux-x64.tar.xz https://developer.salesforce.com/media/salesforce-cli/sf/channels/stable/sf-linux-x64.tar.xz && \
    mkdir -p ~/cli/sf && \
    tar xJf sf-linux-x64.tar.xz -C ~/cli/sf --strip-components 1 && \
    rm sf-linux-x64.tar.xz

# Update PATH environment variable
ENV PATH="/root/cli/sf/bin:${PATH}"

# Copy the Python scripts and requirements
COPY consumer.py .
COPY API.py .
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy supervisord configuration file
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Command to run supervisord
CMD ["supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
