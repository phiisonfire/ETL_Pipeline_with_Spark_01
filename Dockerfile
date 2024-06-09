FROM bde2020/hadoop-base:2.0.0-hadoop3.3.6-java8-ubuntu

SHELL ["/bin/bash", "-c"]

# Install build tools and dependencies
RUN apt-get update && \
    apt-get install -y build-essential python3-dev python3-pip libffi-dev && \
    rm -rf /var/lib/apt/lists/*

RUN printf "<configuration>\n</configuration>\n" > /etc/hadoop/capacity-scheduler.xml \
    && sed -i '/configure \/etc\/hadoop\/mapred-site.xml mapred MAPRED_CONF/ a configure /etc/hadoop/capacity-scheduler.xml capsched CAP_SCHED_CONF' /entrypoint.sh \
    && sed -i 's/addProperty \/etc\/hadoop\/\$module-site.xml \$name \"\$value\"/addProperty \$path \$name \"\$value\"/g' /entrypoint.sh

RUN mkdir -p ~/miniconda3 && \
    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda3/miniconda.sh && \
    bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3 && \
    rm -rf ~/miniconda3/miniconda.sh && \
    ~/miniconda3/bin/conda init bash && \
    ~/miniconda3/bin/conda init zsh && \
    export PATH=/root/miniconda3/bin:$PATH && \
    ~/miniconda3/bin/conda create -n venv python=3.10.14 -y

# Spark
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION_FOR_DOWNLOAD=3
ENV SPARK_DOWNLOAD_URL=https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION_FOR_DOWNLOAD}.tgz

# Download and configure Spark
RUN wget ${SPARK_DOWNLOAD_URL} && \
    tar zxvf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION_FOR_DOWNLOAD}.tgz && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION_FOR_DOWNLOAD}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION_FOR_DOWNLOAD} /opt/spark && \
    echo "export PATH=$PATH:/opt/spark/bin" >> /root/.bashrc && \
    echo "export SPARK_HOME=/opt/spark" >> /root/.bashrc

RUN  echo "export PYSPARK_PYTHON=/root/miniconda3/envs/venv/bin/python" >> /root/.bashrc

RUN mkdir /tmp/spark-events && echo '\
spark.eventLog.enabled          true \n\
spark.eventLog.dir              file:///spark-history \n\
spark.history.fs.logDirectory   file:///spark-history \n\
spark.yarn.historyServer.address 0.0.0.0:18080 \n\
spark.history.ui.port 18080 \n\
' > /opt/spark/conf/spark-defaults.conf

# Download mysql connector driver for Spark
RUN wget -P /opt/spark/jars/ https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar

RUN mkdir /spark-history

RUN echo '\
export HADOOP_CONF_DIR=/opt/hadoop-3.3.6/etc/hadoop \n\
export YARN_CONF_DIR=/opt/hadoop-3.3.6/etc/hadoop \n\
export PYSPARK_PYTHON=/root/miniconda3/envs/venv/bin/python \
' > /opt/spark/conf/spark-env.sh

# Hive
ENV HIVE_VERSION 3.1.3
ENV HIVE_DOWNLOAD_URL https://dlcdn.apache.org/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz
ENV HIVE_HOME /opt/hive
ENV PATH $HIVE_HOME/bin:$PATH
ENV HADOOP_HOME /opt/hadoop-$HADOOP_VERSION

RUN wget ${HIVE_DOWNLOAD_URL} && \
	tar -xzvf apache-hive-$HIVE_VERSION-bin.tar.gz && \
	mv apache-hive-$HIVE_VERSION-bin /opt/hive && \
    rm apache-hive-$HIVE_VERSION-bin.tar.gz && \
    apt-get --purge remove -y wget && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

RUN cp /opt/spark/jars/mysql-connector-j-8.0.33.jar ${HIVE_HOME}/lib/mysql-connector-j-8.0.33.jar

# Spark should be compiled with Hive to be able to use it
# hive-site.xml should be copied to $SPARK_HOME/conf folder
# Note: this added at runtime in entrypoint.sh

# Add hive custom configuration from localhost development to container
ADD ./hive_conf/hive-site.xml $HIVE_HOME/conf
ADD ./hive_conf/beeline-log4j2.properties $HIVE_HOME/conf
ADD ./hive_conf/hive-env.sh $HIVE_HOME/conf
ADD ./hive_conf/hive-exec-log4j2.properties $HIVE_HOME/conf
ADD ./hive_conf/hive-log4j2.properties $HIVE_HOME/conf
ADD ./hive_conf/ivysettings.xml $HIVE_HOME/conf
ADD ./hive_conf/llap-daemon-log4j2.properties $HIVE_HOME/conf



COPY startup.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/startup.sh

COPY entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint.sh

EXPOSE 10000
EXPOSE 10002

ENTRYPOINT ["conda", "run", "-n", "myenv", "/bin/bash", "-c", "/usr/local/bin/entrypoint.sh"]
CMD startup.sh