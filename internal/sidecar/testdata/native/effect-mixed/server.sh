# One server of the mixed-version effect capture (../capture.sh effect-mixed):
# initializes a data directory and runs mysqld in the foreground.
set -euo pipefail
id=$1
mkdir -p /tmp/d
mysqld --no-defaults --user=root --initialize-insecure --datadir=/tmp/d >/tmp/init.log 2>&1
exec mysqld --no-defaults --user=root --datadir=/tmp/d --socket=/tmp/m.sock --pid-file=/tmp/m.pid --log-error=/tmp/m.err \
 --port=3306 --bind-address=0.0.0.0 --mysqlx=OFF --server-id="$id" --log-bin=/tmp/d/mysql-bin --gtid-mode=ON --enforce-gtid-consistency=ON \
 --log-replica-updates=ON --binlog-format=ROW --binlog-row-image=FULL --replica-preserve-commit-order=ON --skip-replica-start --innodb-buffer-pool-size=64M
