FROM openjdk:14-alpine
RUN apk --update-cache \
    add python3 openssh iperf3
RUN ssh-keygen -A \
    && sed -ie "s/root:!/root:*/" /etc/shadow \
    && sed -ie "s/#Port 22/Port 2222/" /etc/ssh/sshd_config \
    && sed -ie "s/#StrictModes yes/StrictModes no/" /etc/ssh/sshd_config \
    && sed -ie "s/#PasswordAuthentication yes/PasswordAuthentication no/" /etc/ssh/sshd_config \
    && sed -ie "s/#ChallengeResponseAuthentication yes/ChallengeResponseAuthentication no/" /etc/ssh/sshd_config \
    && echo -e "JAVA_HOME=/opt/openjdk-14\nPATH=\$JAVA_HOME/bin:\$PATH" > /etc/profile.d/java.sh
COPY IntegrationTest/test-keys/id_rsa.pub /root/.ssh/authorized_keys
RUN chmod 700 /root/.ssh \
    && chmod 600 /root/.ssh/authorized_keys
COPY lib /root/lib
COPY bin /root/bin
COPY config /root/config
COPY runscripts /root/runscripts
CMD ["/usr/sbin/sshd", "-D"]