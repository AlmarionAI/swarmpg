# PgBouncer from Debian's package, for the benchmark baseline.
FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends pgbouncer \
    && rm -rf /var/lib/apt/lists/*
COPY bench/pgbouncer.ini /etc/pgbouncer/pgbouncer.ini
COPY bench/userlist.txt /etc/pgbouncer/userlist.txt
USER postgres
EXPOSE 6432
ENTRYPOINT ["pgbouncer", "/etc/pgbouncer/pgbouncer.ini"]
