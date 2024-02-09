FROM ubuntu:latest
RUN apt-get update && apt-get install -y cron python3.10 python3-pip
WORKDIR /app
COPY . /app
RUN pip3 install --no-cache-dir -r requirements.txt
RUN echo "0 0 * * * python3 /app/main.py" > /etc/cron.d/my_cron_job
RUN chmod 0644 /etc/cron.d/my_cron_job
RUN crontab /etc/cron.d/my_cron_job
CMD ["cron", "-f"]
