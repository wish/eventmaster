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
- Either one of the following
  - Postgres (preferred), or
  - Cassandra (deprecated)

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

### Database Connection
Eventmaster currently supports two database backends: Postgres (preferred) and Cassandra (deprecated). The choice of database needs to supplied in the `data_store` field of the config file.

To connect to a Postgres database, serveral fields have to be set in the configration file. This includes:
  - `data_store`: your choice of database. Set to `postgres` to connect to a postgres database
  - `postgres_config`: object containing postgres configs
    - `addr`: address of the postgres database
    - `port`: port of the postgres database
    - `database`: the name of the database
    - `username`: the username of the user with necessary permission to connect/read/write, and
    - `password`: the password of the user (ommited if Vault integration is turned on)

    Additionally, you may choose to toggle integration with HasiCorp Vault. This allows Eventmaster to obtain database password from Vault instead of leaving them as plain-text in the filesystem. Only v2 is supported currently. To do so, set the following fields:
    - `vault`: object containing Vault config
      - `enabled`: set to true to enable Vault integration
      - `addr`: API endpoint of Vault
      - `token`: token used to connect to Vault
      - `path`: path to the password

Although no longer recommended, you can connect to a Cassandra database by setting the following fields:
  - `data_store`: set to `cassandra` to connect to a c* database
    - `addrs`: array of C* database addresses
    - `keyspace`: name of the keyspace you wish to use
    - `consistency`: consistency level setting, default to be `one`,
    - `timeout`: connection timeout

    To connect to a Cassandra database over TLS, several fields have to be set in the configration file. This includes:
    - `secured`: Setting this field to `true` triggers TLS
    - `ca_path`: The path to the CA cert file
    - `port`: If different than default native protocol clients (port 9042)
    - `username`
    - `password`

An example of the config file can be found [here](https://github.com/wish/eventmaster/blob/master/etc/eventmaster.json)
### Database Setup

Execute `schema.sql` or `schema.cql` from the `schema` directory on your database cluster. This will set up the tables needed.

### Tests
Tests can be run (using the go tool) by calling:

```
$ make test
```

## Adding annotations to a Grafana dashboard

This is documented in the [documentation subdirectory](./docs/grafana)

## REST API

This is documented in the [documentation subdirectory](./docs/api/).
