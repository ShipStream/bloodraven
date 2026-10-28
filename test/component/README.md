# Component tests

Run the normal component gate from the repository root:

```bash
make test-component
```

Tenant database tests use the production reconciler with an in-memory SQL
driver. The driver models explicit and global privileges, partial restrictions,
restricted grantors, lowercase account hosts and grant-only schema rows. These
tests run in CI without MySQL.

## Optional real-MySQL tenant grant tests

Set `BLOODRAVEN_TENANT_TEST_ADDR` to run the real-MySQL tests and the `mysql`
subtests of the review regressions. They skip when the variable is absent.
The tests require MySQL with `partial_revokes=ON` and an empty root password.
Use a disposable server: fixtures create and remove `acme_wms`, tenant accounts
and operator credentials, and the global-grants test changes root's grants.

Start a local server, wait for its TCP listener, run the tests, then remove it:

```bash
docker run --rm -d --name bloodraven-tenant-tests \
  -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -p 127.0.0.1::3306 \
  mysql:8.4 --partial-revokes=ON
docker exec bloodraven-tenant-tests mysqladmin \
  --host=127.0.0.1 --protocol=TCP --wait=30 ping --silent
BLOODRAVEN_TENANT_TEST_ADDR="$(docker port bloodraven-tenant-tests 3306/tcp)" \
  go test -race ./test/component -run '^TestMysqlDatabase' -count=1 -v
docker rm -f bloodraven-tenant-tests
```

The real-server regressions cover shared/operator/root global access,
restricted-grantor repair, mixed-case IPv6 hosts, narrowing, retirement, deletion,
and retries after failed Ready/finalizer persistence. To also check database-name
folding, repeat with `--lower-case-table-names=1` added to the MySQL arguments.
