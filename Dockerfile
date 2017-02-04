FROM fedora

RUN dnf install python-pip -y
RUN pip install --upgrade pip
RUN pip install requests
RUN pip install redis
RUN pip install gevent

ENV PATH="/usr/local/RedisWeed:${PATH}"