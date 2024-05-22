# Use the custom made image zyth python and salesforce-cli
FROM gillm/pythonv13.2-slim_salesforce-cli2.30.8

WORKDIR /app

# Update PATH environment variable
ENV PATH="/root/cli/sf/bin:${PATH}"

COPY . .

RUN ls -la

# Install Python dependencies
RUN pip install -r config/requirements.txt

# Copy supervisord configuration file
COPY config/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Command to run supervisord
CMD ["supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]


