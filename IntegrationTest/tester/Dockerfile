FROM alpine
RUN apk --update-cache \
    add python3 openssh tmux
RUN ssh-keygen -A \
    && sed -ie "s/root:!/root:*/" /etc/shadow \
    && sed -ie "s/#Port 22/Port 2222/" /etc/ssh/sshd_config \
    && sed -ie "s/#StrictModes yes/StrictModes no/" /etc/ssh/sshd_config \
    && sed -ie "s/#PasswordAuthentication yes/PasswordAuthentication no/" /etc/ssh/sshd_config \
    && sed -ie "s/#ChallengeResponseAuthentication yes/ChallengeResponseAuthentication no/" /etc/ssh/sshd_config \
    && sed -ie "s/#   StrictHostKeyChecking ask/StrictHostKeyChecking no/" /etc/ssh/ssh_config
COPY test-keys /root/test-keys
RUN mkdir /root/.ssh \
    && cp /root/test-keys/id_rsa.pub /root/.ssh/authorized_keys \
    && chmod 700 /root/.ssh \
    && chmod 600 /root/.ssh/authorized_keys \
    && chmod 600 /root/test-keys/id_rsa
COPY tools /root/tools
RUN chmod +x /root/tools/open-shells.sh
CMD ["/usr/sbin/sshd", "-D"]