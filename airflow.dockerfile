FROM marcelmittelstaedt/airflow:latest

RUN rm -rf /home/airflow/BigData
RUN rm -rf /home/airflow/airflow/dags/dags
RUN rm -rf /home/airflow/airflow/python/python
RUN rm -rf /home/airflow/airflow/plugins/python

ADD ./requirements.txt /home/tmp/python/

WORKDIR /home/tmp/python/
RUN pip3 install -r requirements_airflow.txt

WORKDIR /

# Switch back to Root User
USER root
WORKDIR /

COPY startup.sh /startup.sh
RUN chmod +x /startup.sh

# Expose Airflow Web Service Port
EXPOSE 8080

# Start startup Script
ENTRYPOINT ["/startup.sh"]
