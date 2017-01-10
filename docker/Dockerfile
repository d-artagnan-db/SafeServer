# Use phusion/baseimage as base image. 

FROM phusion/baseimage:0.9.18

# Use baseimage-docker's init system.
CMD ["/sbin/my_init"]

# ...put your own build instructions here...
RUN apt-get update --assume-yes --quiet && apt-get install python-software-properties --assume-yes --quiet
RUN \
  echo oracle-java7-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
  add-apt-repository -y ppa:webupd8team/java && \
  apt-get update && \
  apt-get install -y oracle-java7-installer --quiet --assume-yes

RUN apt-get install oracle-java7-set-default --assume-yes --quiet

WORKDIR /usr/local/hbase
RUN wget http://mirrors.fe.up.pt/pub/apache/hbase/0.98.22/hbase-0.98.22-hadoop2-bin.tar.gz --quiet
RUN tar -xvzf hbase-0.98.22-hadoop2-bin.tar.gz
RUN rm -f hbase-0.98.22-hadoop2-bin.tar.gz

WORKDIR /usr/local/hbase/hbase-0.98.22-hadoop2/conf
RUN sed -i "s|/usr/java/jdk1.6.0|/usr/lib/jvm/java-7-oracle|g" hbase-env.sh
RUN sed -i "s/# export JAVA_HOME/export JAVA_HOME/g" hbase-env.sh

ENV PATH /usr/local/hbase/hbase-0.98.22-hadoop2/bin:$PATH
ENV HBASE_HOME /usr/local/hbase/hbase-0.98.22-hadoop2

WORKDIR /root

COPY saferegions.sh .
COPY configurations/* .


COPY *.jar /usr/local/hbase/hbase-0.98.22-hadoop2/lib/	

ENTRYPOINT ["./saferegions.sh"]

# Clean up APT when done.
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && rm -rf /var/cache/oracle-jdk7-installer
