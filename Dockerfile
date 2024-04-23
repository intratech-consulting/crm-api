# Use the custom made image zyth python and salesforce-cli
FROM gillm/pythonv13.2-slim_salesforce-cli2.30.8

WORKDIR /app

# Update PATH environment variable
ENV PATH="/root/cli/sf/bin:${PATH}"

# Copy the Python scripts and requirements
COPY consumer.py .
COPY API.py .
COPY requirements.txt .
COPY salesforce.key .
COPY heartbeat.py .
COPY sender_users.py .
COPY sender_companies.py .
COPY sender_talks.py .
COPY sender_attendances.py .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy supervisord configuration file
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Command to run supervisord
CMD ["supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]

