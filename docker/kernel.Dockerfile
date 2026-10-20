# SwarmPG kernel: upstream PostgreSQL at kernel/BASE plus kernel/patches, with the
# agent schema applied on first start. --build-arg UPSTREAM=1 builds the same
# commit without the patches, for like-for-like comparisons.
FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential git ca-certificates bison flex pkg-config libreadline-dev zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*
COPY kernel /src/kernel
ARG UPSTREAM=
RUN SWARMPG_UPSTREAM=$UPSTREAM SWARMPG_SRC=/build/pg PREFIX=/opt/swarmpg CONFIGURE_FLAGS="" /src/kernel/build.sh fetch \
    && SWARMPG_SRC=/build/pg PREFIX=/opt/swarmpg CONFIGURE_FLAGS="" /src/kernel/build.sh build

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libreadline8 zlib1g \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --home-dir /var/lib/swarmpg postgres
COPY --from=build /opt/swarmpg /opt/swarmpg
COPY sql/schema.sql /opt/swarmpg/share/swarmpg/schema.sql
COPY docker/kernel-entrypoint.sh /usr/local/bin/swarmpg-entrypoint
ENV PATH=/opt/swarmpg/bin:$PATH PGDATA=/var/lib/swarmpg/data
USER postgres
EXPOSE 5432
ENTRYPOINT ["swarmpg-entrypoint"]
