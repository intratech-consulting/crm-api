# Use the custom made image zyth python and salesforce-cli
FROM gillm/pythonv13.2-slim_salesforce-cli2.30.8

WORKDIR /app

# Update PATH environment variable
ENV PATH="/root/cli/sf/bin:${PATH}"

# Copy the Python scripts and requirements
COPY src/consumer.py .
COPY src/API.py .
COPY config/requirements.txt .
COPY config/salesforce.key .
COPY src/heartbeat.py .
COPY src/sender_users.py .
COPY src/sender_companies.py .
COPY src/sender_talks.py .
COPY src/sender_attendances.py .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy supervisord configuration file
COPY config/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Command to run supervisord
CMD ["supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]


