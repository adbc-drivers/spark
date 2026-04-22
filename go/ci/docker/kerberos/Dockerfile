FROM debian:bookworm AS kerberos

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get -qq update
RUN apt-get -qq install locales net-tools krb5-kdc krb5-admin-server
RUN apt-get -qq clean

RUN locale-gen "en_US.UTF-8"
RUN echo "LC_ALL=\"en_US.UTF-8\"" >> /etc/default/locale

ENV REALM ${REALM:-KDC.LOCAL}

COPY ./conf/ /etc/
RUN mkdir -p /var/log/kerberos/

ENV KADMIN_PRINCIPAL=admin
ENV KADMIN_PASSWORD=password

COPY entrypoint.sh /
ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]

EXPOSE 749 750 88
