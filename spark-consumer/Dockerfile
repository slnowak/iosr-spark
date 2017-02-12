FROM epahomov/docker-spark:spark_2.1_hadoop_2.7

#install necessary depdencencies
RUN add-apt-repository ppa:fkrull/deadsnakes -y
RUN apt-get update && apt-get install -y python3.4 python3-pip

#prepare script and env
COPY *.py ./
COPY requirements ./
RUN python3.4 -m pip install -r requirements
ENV PYSPARK_PYTHON python3.4

#set up recurrent job
COPY run_recurrently.sh ./
RUN chmod +x run_recurrently.sh
CMD ["/spark/run_recurrently.sh"]
