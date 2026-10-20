#!/bin/sh
# First start: create the cluster and the swarmpg database with the agent schema.
# Development quickstart only: every connection on the container network is trusted.
set -eu

if [ ! -s "$PGDATA/PG_VERSION" ]; then
	initdb -D "$PGDATA" -U postgres --auth=trust >/dev/null
	echo "listen_addresses = '*'" >>"$PGDATA/postgresql.conf"
	echo "host all all all trust" >>"$PGDATA/pg_hba.conf"
	pg_ctl -D "$PGDATA" -o "-c listen_addresses='' -c unix_socket_directories=/tmp" -w start >/dev/null
	createdb -h /tmp -U postgres swarmpg
	psql -h /tmp -U postgres -d swarmpg -v ON_ERROR_STOP=1 -q -f /opt/swarmpg/share/swarmpg/schema.sql
	pg_ctl -D "$PGDATA" -m fast -w stop >/dev/null
	echo "swarmpg: cluster initialised with the agent schema (trust auth: development only)"
fi

# With PRINCIPAL_SECRET set, agent principals must be signed with it (the gateway does).
exec postgres -D "$PGDATA" -c unix_socket_directories=/tmp \
	${PRINCIPAL_SECRET:+-c agent_principal_secret=$PRINCIPAL_SECRET}
