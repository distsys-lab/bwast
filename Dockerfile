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
COPY bft-smart/lib /root/lib
COPY bft-smart/bin /root/bin
COPY bft-smart/config /root/config
COPY bft-smart/runscripts /root/runscripts
CMD ["/usr/sbin/sshd", "-D"]