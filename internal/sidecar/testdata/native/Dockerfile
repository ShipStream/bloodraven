FROM mysql:8.4
RUN printf '[mysql84-client]\nname=MySQL 8.4 client\nbaseurl=https://repo.mysql.com/yum/mysql-8.4-community/el/9/$basearch/\nenabled=1\ngpgcheck=1\ngpgkey=https://repo.mysql.com/RPM-GPG-KEY-mysql-2025\n' > /etc/yum.repos.d/mysql84-client.repo \
    && microdnf remove -y mysql-community-server-minimal \
    && microdnf install -y mysql-community-client-8.4.11-1.el9 mysql-community-client-plugins-8.4.11-1.el9 mysql-community-common-8.4.11-1.el9 mysql-community-libs-8.4.11-1.el9 && microdnf clean all
ENV TZ=UTC
ENTRYPOINT ["mysqlbinlog"]
