# Quiz — Building a group from nothing

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

## Question 1

**Type:** MULTIPLE_CHOICE

You apply a brand-new three-site `MysqlFailoverGroup` into an empty namespace. All three MySQL pods come up, and every one of them reports `read_only=0`. From Unit 2 you know the matrix flags `SPLIT BRAIN` the moment more than one core site is writable. What does the operator actually do?

- Nothing alarming: `isFreshDeploy` finds every site writable, replication never configured and no data anywhere, so it seeds a primary by `sitePriorities` and clones the others from it
- It reports `SPLIT BRAIN` and waits for a human, which is why a new group must be applied one site at a time
- It promotes the first site in `spec.sites` and fences the rest, because declaration order breaks the tie on a fresh cluster
- It reports `NoPrimary`, because a site that has never replicated cannot be treated as an authority

**Correct option index:** 0

**Explanation:**

The split-brain path is real, and a *separate*, deliberately conservative check runs ahead of it. `isFreshDeploy` demands three things of every site at once — writable, no replication metadata, and no data — and only then seeds the group. Option 2 describes what would happen without that check and would make every new group a manual procedure. Option 3 is the dangerous misreading: priority order does decide the seed, but only *after* emptiness has been proved, and skipping that proof is exactly how a restart-amnesia cluster gets cloned backwards. Option 4 confuses `NoPrimary`, which needs every core site read-only, with an all-writable topology. (objective 1)

## Question 2

**Type:** MULTIPLE_CHOICE

Your platform team hands you three PVCs that already contain a working MySQL dataset, and asks you to put a `MysqlFailoverGroup` in front of them. What do you have to do that a green-field install would have done for you?

- Create the replication user with `REPLICATION SLAVE, REPLICATION CLIENT, BACKUP_ADMIN, CLONE_ADMIN`, install the clone plugin, and create any credentials-mode users by hand — the init script only runs on an empty datadir
- Nothing: the init-users ConfigMap is applied on every pod start, so the users appear on the first restart
- Set `spec.credentials.operatorSecret` and restart the pods; the operator reconciles MySQL users on every poll
- Delete the PVCs and let the operator bootstrap, because adopting an existing datadir is not supported

**Correct option index:** 0

**Explanation:**

The init-users ConfigMap is mounted at `/docker-entrypoint-initdb.d`, which the MySQL entrypoint runs only when it is initialising an empty datadir. Adopt a populated one and none of it runs. `CLONE_ADMIN` and `BACKUP_ADMIN` are the two grants people miss, and their absence surfaces much later as a reclone that will not start or a backup job that cannot take a consistent dump. Option 2 misremembers the entrypoint contract. Option 3 invents a reconcile loop over MySQL users that does not exist for this path. Option 4 is over-cautious: adoption works, it just moves the user setup onto you. (objectives 1, 2)

## Question 3

**Type:** MULTIPLE_CHOICE

A security review asks which MySQL user your application connects as, and what it could do if the credentials leaked. You are in credentials mode with `appSecret` set. What is the honest answer?

- It holds `ALL PRIVILEGES ON *.*` but without `GRANT OPTION` and without `SUPER`, so it can read and write everything and cannot create users, grant privileges or bypass `super_read_only`
- It holds only the grants you listed in `spec.credentials`, so the answer depends on your manifest
- It holds `SELECT, SHOW VIEW, SHOW DATABASES, PROCESS` — the application user is read-only by default
- It is the same account the operator uses, so it holds `ALL PRIVILEGES` with `GRANT OPTION`

**Correct option index:** 0

**Explanation:**

The grant sets are fixed by the operator, not configurable, and that is precisely what makes them answerable. The app user is deliberately full-privilege-minus-escalation: it cannot grant, and without `SUPER`/`CONNECTION_ADMIN` it cannot write through a `super_read_only` fence. Option 2 is the answer people expect and is wrong — there is no grant list in the CRD. Option 3 describes `readOnlySecret`. Option 4 describes `operatorSecret`, and conflating the two is the actual finding a review is looking for. (objective 2)

## Question 4

**Type:** TRUE_FALSE

Rotating the password in a credentials-mode Secret changes the password MySQL will accept for that user.

**Correct answer:** false

**Explanation:**

It does not. The `CREATE USER IF NOT EXISTS` / `ALTER USER` pair lives in the init script, which the MySQL entrypoint runs only on an empty datadir. Changing the Secret changes what the operator and sidecar *present*; MySQL still expects the old password, and the site drops to `unreachable` with no obvious cause. What the rotation *does* do is change the spec hash — credential Secret data is folded into it — so the pods roll, which makes it look like the change took effect. Rotate the password inside MySQL as well, or the restart is the only thing you achieved. (objective 2)

## Question 5

**Type:** SHORT_ANSWER

You are reviewing a colleague's first `MysqlFailoverGroup` before they apply it. It sets both `secretName` and `credentials`; gives one site a `taintNodeSelector` naming labels no node carries; lists a `storageClassName` that does not exist in the cluster; and sets `image: mysql:9`. Which of these will they find out about from `kubectl apply`, and which will they find out about later — and for the later ones, how?

**Sample answer:**

Only the first is caught at apply time: a CEL rule refuses an object that sets both `secretName` and `credentials` — *exactly one of secretName or credentials must be set* — so the apply fails immediately and costs them seconds. The other three are admitted. The bogus `taintNodeSelector` fails silently and completely: the operator selects nodes by those labels, matches nothing, and applies the `NoExecute` taint to no node, so the eviction half of their failover strategy does not exist and nothing anywhere says so. The missing `storageClassName` shows up as PVCs stuck `Pending` and pods stuck `ContainerCreating` — visible within a minute, if they look. And `image: mysql:9` is admitted because there is no version admission check of any kind; a floating tag can drift them onto an unsupported MySQL between pod restarts, and it surfaces as MySQL pod failures rather than an operator error.

**A full-credit answer shows:**

A strong answer separates the four correctly: (1) both-credentials-modes is a CEL rejection at apply; (2) the bad `taintNodeSelector` is silent and permanent, and names the consequence — no taint is applied, so nothing is evicted at the demoted site; (3) the missing StorageClass is visible as `Pending` PVCs; (4) the floating tag is admitted because no version admission check exists, and the risk is drifting onto an unsupported version between restarts. Credit an answer that notes the general rule: admission catches structural mistakes about the object, and catches nothing about the cluster the object refers to.

**Explanation:**

The split is the whole point of the topic. CEL rules validate the *object* — uniqueness, mutual exclusion, required fields per role, the interval relationships on `spec.sidecar` — and they are cheap and immediate. Nothing validates the object against the *cluster*: not node labels, not storage classes, not image tags, not whether the Secret's credentials are ones MySQL actually has. The `taintNodeSelector` case is the one worth remembering, because unlike a `Pending` PVC it produces no symptom at all until a failover that should have evicted an application pod quietly does not. (objective 3)
