# Objects as v1.7.0 stored them

One real object per kind, read back through `shipstream.io/v1alpha1` from a
cluster that ran the **published v1.7.0** chart and images (a kind cluster
installed by `./playground/upgrade-e2e.sh install`, captured on 2026-11-16).
The v1.7.0 operator wrote every `status` here:

| File | What it is |
|---|---|
| `mysqlfailovergroup.yaml` | The three-site playground group, healthy, with Dragonfly and a `rustfs` backup profile. |
| `mysqldatabase.yaml` | A tenant database, `Ready`. |
| `mysqlbackup.yaml` | A backup to S3, `Succeeded`. |
| `mysqlbackupverification.yaml` | A verification of that backup, `Succeeded`. |
| `mysqlstandbycluster.yaml` | A standby cluster reading the same bucket. |

Server-set metadata was removed (`uid`, `resourceVersion`, `generation`,
`creationTimestamp`, `managedFields`, `namespace`, owner references and
kubectl's last-applied annotation). Nothing else was edited.

They are the backward-compatibility fixtures of the v1beta1 promotion
(C-6 of `proposals/20-api-v1beta1.md`). The tests that read them assert
that an object stored by v1.7.0:

- decodes strictly into this build's Go types, with no unknown field;
- conforms to this build's schema at both served versions;
- reads back complete through `shipstream.io/v1beta1` after this build's
  CRDs are applied over v1.7.0's;
- is rewritten by `kubectl bloodraven migrate-storage` without being
  deleted, recreated or changed.

**Do not regenerate or edit these files to make a test pass.** A failure
means a schema change broke objects that exist in clusters. Either keep
the change compatible, or it needs a conversion webhook and a documented
migration first.
