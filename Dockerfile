FROM openjdk:11-slim-bullseye
RUN apt-get update && apt-get install -y \
    python3 \
    python3-distutils \
    openssh-server \
    iproute2 \
    iperf3 \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py \
    && python3 get-pip.py \
    && rm get-pip.py \
    && apt-get purge -y curl \
    && pip3 install --no-cache-dir tcconfig
RUN ssh-keygen -A \
    && mkdir -p /run/sshd \
    && sed -ie "s/root:!/root:*/" /etc/shadow \
    && sed -ie "s/#Port 22/Port 2222/" /etc/ssh/sshd_config \
    && sed -ie "s/#StrictModes yes/StrictModes no/" /etc/ssh/sshd_config \
    && sed -ie "s/#PasswordAuthentication yes/PasswordAuthentication no/" /etc/ssh/sshd_config \
    && sed -ie "s/#ChallengeResponseAuthentication yes/ChallengeResponseAuthentication no/" /etc/ssh/sshd_config
RUN echo "JAVA_HOME=/usr/local/openjdk-11\nPATH=\$JAVA_HOME/bin:\$PATH" > /etc/profile.d/java.sh
COPY lib /root/lib
COPY bin /root/bin
COPY config /root/config
COPY runscripts /root/runscripts
CMD ["/usr/sbin/sshd", "-D"]