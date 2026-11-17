# Objects as v1.6.0 accepted them

One object per kind, written for the schema of the published v1.6.0 CRDs
(`../crd-v1.6.0`). v1.7.0 renamed and reshaped most of these fields, and
there is no in-place upgrade from v1.6.0. The envtests create them under
the v1.6.0 CRDs, apply this build's CRDs over those, and assert that
`kubectl bloodraven migrate-storage` and the operator's check at start
refuse them without writing anything.

What makes each one not fit today's schema:

| File | Under today's schema |
|---|---|
| `mysqlfailovergroup.yaml` | Stored defaults changed type (`cloneTimeout: 3600`, `updateStrategy: OrderedUpdate`), and `credentials`, `dns` and `sites` lost their old field names. |
| `mysqldatabase.yaml` | Only a required field is missing: `groupRef` became `failoverGroupRef`. |
| `mysqlstandbycluster.yaml` | Only required fields are missing: `source.storage` became `source.s3`. No `MysqlFailoverGroup` is needed for it to exist. |
| `mysqlbackup.yaml` | Conforms: its fields kept their names. |
| `mysqlbackupverification.yaml` | Conforms. |

Do not edit them to match a newer schema.
