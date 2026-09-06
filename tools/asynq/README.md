# Asynq CLI

`asynq` is the terminal client for inspecting and administering queues, tasks,
groups, cron entries, and servers created by
[`github.com/pars-aria-labs/asynq`](https://github.com/pars-aria-labs/asynq).
Use the same Redis endpoint, credentials, database, and key prefix as your
producers and workers.

## Install

The CLI is a separate Go module. Pin it to the release used by your services:

```sh
go install github.com/pars-aria-labs/asynq/tools/asynq@v0.27.1
```

Go installs the executable in `GOBIN`, or in `$GOPATH/bin` when `GOBIN` is not
set. Confirm the installed version with:

```sh
asynq version
```

## Commands

Run `asynq <command> <subcommand> --help` for the complete flags and examples
for an operation.

| Command | Subcommands | Purpose |
| --- | --- | --- |
| `cron` | `list`, `history` | Inspect scheduler entries and their history |
| `dash` | — | Open the interactive terminal dashboard |
| `group` | `list` | List aggregation groups for a queue |
| `queue` | `list`, `inspect`, `history`, `remove`, `pause`, `resume` | Inspect and administer queues |
| `server` | `list` | List active Asynq servers |
| `stats` | — | Show the current aggregate state |
| `task` | `list`, `inspect`, `enqueue`, `cancel`, `delete`, `archive`, `run`, `deleteall`, `archiveall`, `runall` | Inspect and administer tasks |

For example:

```sh
asynq queue list
asynq task list --queue=critical --state=archived
asynq task inspect --queue=critical --id=TASK_ID
asynq group list --queue=critical
asynq cron list
```

To enqueue a JSON task from a shell, pass JSON as one quoted argument so the
shell does not split it:

```sh
asynq task enqueue \
  --type_name=email:welcome \
  --payload='{"user_id":42}' \
  --queue=critical \
  --retry=5
```

## Redis connection flags

Connection flags are global and may be placed before or after a subcommand.

| Flag | Default | Meaning |
| --- | --- | --- |
| `-u`, `--uri` | `127.0.0.1:6379` | Standalone Redis address in `host:port` form |
| `-n`, `--db` | `0` | Redis database for a standalone connection |
| `-U`, `--username` | empty | Redis ACL username |
| `-p`, `--password` | empty | Redis password |
| `--prefix` | empty | The exact `RedisClientOpt.Prefix` used by the application |
| `--cluster` | `false` | Use Redis Cluster instead of a standalone connection |
| `--cluster_addrs` | six local addresses on ports 7000–7005 | Comma-separated cluster seed addresses |
| `--tls` | `false` | Enable TLS |
| `--tls_server` | empty | Server name used for TLS certificate validation; also enables TLS |
| `--insecure` | `false` | Skip TLS certificate validation; intended only for controlled development |
| `--config` | `$HOME/.asynq.yaml` | Read defaults from a YAML, JSON, or other Viper-supported config file |

The CLI does not discover application settings. A mismatched database or
prefix looks like an empty Asynq installation even when tasks exist elsewhere
in Redis.

### Standalone Redis with a prefix

If the application uses `Prefix: "billing-prod"`, pass the same value:

```sh
asynq queue list \
  --uri=127.0.0.1:6379 \
  --db=2 \
  --prefix=billing-prod
```

The prefix is a namespace, not a queue name. Do not include the generated
`:asynq:` portion. Prefixes whose first Redis hash-tag is empty, such as
`tenant{}`, are rejected because they cannot safely group queue keys in Redis
Cluster.

### Redis Cluster

Redis Cluster supports database 0 only, so `--db` is not used in cluster mode:

```sh
asynq stats \
  --cluster \
  --cluster_addrs=redis-0:7000,redis-1:7001,redis-2:7002 \
  --prefix=billing-prod
```

Do not put spaces between seed addresses. Every key for a queue is assigned a
cluster hash tag by Asynq; the prefix must match the one used by producers and
workers.

### TLS and ACL authentication

For a certificate issued to `redis.internal.example`:

```sh
asynq server list \
  --uri=redis.internal.example:6380 \
  --username=observer \
  --tls_server=redis.internal.example \
  --prefix=billing-prod
```

Prefer a protected config file or your process supervisor for secrets instead
of putting `--password` in shell history. `--insecure` disables certificate
verification and should not be used in production.

## Config file

By default, the CLI looks for `$HOME/.asynq.yaml` (and other formats supported
by Viper). Use `--config=/path/to/file.yaml` to select another file. Flag values
override config defaults.

```yaml
uri: redis.internal.example:6380
db: 2
username: observer
password: replace-with-a-secret
prefix: billing-prod
tls: true
tls_server: redis.internal.example
insecure: false
```

A cluster configuration uses the same key names as the long flags:

```yaml
cluster: true
cluster_addrs: redis-0:7000,redis-1:7001,redis-2:7002
prefix: billing-prod
username: observer
password: replace-with-a-secret
tls: true
```

Keep configuration files containing credentials outside source control and
restrict their filesystem permissions.
