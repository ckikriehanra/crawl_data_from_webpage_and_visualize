FROM apache/airflow:2.6.0
COPY ./requirements.txt ./requirements.txt
RUN pip install -r ./requirements.txt
USER root
# Install OpenJDK-11
RUN apt update && \
    sudo apt install wget && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

# Update the base image and install a newer version of Python
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    pip3 install --upgrade pip




