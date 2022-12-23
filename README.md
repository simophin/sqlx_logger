# sqlx_logger

Receives uncompressed GELF log from the UDP socket and writes them to an SQL db.

## Motivation

`Loki` sucks. 

The log scraper `promtail` works fine, but `LogQL` sucks big time: it doesn't handle
"-" in the JSON field well. Got very frustrated about how to do that easily.

And the language itself is yet another thing to learn. Nothing against it just wanna 
use my plan old SQL.

Then I thought, why can't I collect the log and write them in an SQL db instead? Hence
this program.

## How it works

It's very easy. This app listens on a specific UDP port for [GELF](https://go2docs.graylog.org/5-0/getting_in_log_data/gelf.html) messages, then write it to the SQL you specified. Like this:

```bash
./sqlx_logger --db-url "postgres://user:pass@host:port/db" "INSERT INTO logs(body) VALUES ($1)" --filter json --listen "0.0.0.0:9000"
```

Supported db:
- PostgreSQL
- MSSQL
- MySQL
- Sqlite

Basically whatever `sqlx` supported. See [sqlx](https://github.com/launchbadge/sqlx) for 
connection strings.

## How to collect logs from docker?
See [Docker GELF driver](https://docs.docker.com/config/containers/logging/gelf/) to enable GELF
for docker containers. Note: this log collector only supports UDP and uncompressed logs. Example
configuration:

### `/etc/docker/daemon.json`

```json
{
  "log-driver": "gelf",
  "log-opts": {
    "gelf-address": "udp://192.168.1.1:9000",
    "gelf-compression-type": "none"
  }
}
```