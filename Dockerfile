FROM fedora

RUN dnf install python-pip -y
RUN pip install --upgrade pip
RUN pip install requests
RUN pip install redis
RUN pip install gevent

ENV PATH="/usr/local/RedisWeed:${PATH}"

# command: docker run -d -v /root/weed_expire/RedisWeed:/usr/local/RedisWeed --name wx weedex listen_expire
# --redis-addr=10.0.83.91 --redis-port=6379 --weeds-addr=10.0.83.159 --weeds-port=9333