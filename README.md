# eventmaster

[![build status](https://github.com/wish/eventmaster/actions/workflows/build.yml/badge.svg)](https://github.com/wish/eventmaster/actions)

There are lots of events that happen in production such as deploys, service
restarts, scale-up operations, etc. Right now these events happen, but we have
no centralized place to track them -- which means we have no centralized place
to search them. This project implements a service that:

- Creating an event store
- API for sending/firing events
- Potential integrations into other services for searching etc. (e.g. [grafana annotations](./docs/grafana))
- Working with other pieces of infrastructure to expose meaningful events

## Setup
### Dependencies
- Go tool (tested at v1.16)
- Cassandra v3.10

### Get Code
```
$ git clone \
    git@github.com:wish/eventmaster.git \
    $GOPATH/src/github.com/wish/eventmaster
```

### Building

Dependencies are currently fetched using [Go Modules](https://blog.golang.org/using-go-modules).
These are set up as dependencies to the default make target, so running:

```
make
```

will emit `$GOPATH/bin/eventmaster` after fetching and compiling dependencies.


### Running

After running `make` `eventmaster` can be called directly:

```bash
$ $GOPATH/bin/eventmaster
```

Giving no flags `eventmaster` runs with sane option.

### Configuration

Various aspects of `eventmaster` can be controlled through a collection of
flags and options in a json config file. This will be consolidated when
[issue #32](https://github.com/wish/eventmaster/issues/32)
is resolved.

The config provided in `etc/eventmaster.json` encodes the default values
assumed by `eventmaster` if run without specifying a config file (`-c`). If
these settings do not work for your service, please modify
`etc/eventmaster.json` to specify values such as the addresses of the Cassandra
cluster along with other database options.

Of note if `"cassandra_config":"service_name"` is non-empty then `eventmaster`
currently uses
[discovery](https://github.com/wish/discovery)
to find the IPs of the Cassandra cluster.

For example the port of the eventmaster server can be configured using the
`--port` option, and if an alternate cassandra address needs to be specified
adjust a `eventmaster.json` file and specify that it is used by providing the
`-c` flag.

Open the eventmaster UI in your browser (default: `http://localhost:50052`).
The UI can be used to create and query topics, data centers, and events.

To connect to a Cassandra database over TLS, several fields have to be set in the configration file. This includes:
- `secured`: Setting this field to `true` triggers TLS
- `ca_path`: The path to the CA cert file
- `port`: If different than default native protocol clients (port 9042)
- `username`
- `password`
### Database Setup

Execute `schema.cql` on your Cassandra cluster. This will set up the
`event_master` keyspace and `event`, `temp_event`, `event_topic`, and
`event_dc` tables.

```bash
$ cqlsh -f schema.cql
```

The `class` and `replication_factor` can be modified as long as the
`consistency` attribute is configured accordingly in the `eventmaster.json`
used by `eventmaster -c eventmaster.json`.

### Tests
Tests can be run (using the go tool) by calling:

```
$ make test
```

## Adding annotations to a Grafana dashboard

This is documented in the [documentation subdirectory](./docs/grafana)

## REST API

This is documented in the [documentation subdirectory](./docs/api/).
