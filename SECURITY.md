# Security policy

SwarmPG exists to isolate agents from each other, so a way around that isolation is the
most serious bug the project can have.

## Reporting a vulnerability

Report privately through GitHub: **Security → Report a vulnerability** on this repository.
Do not open a public issue or pull request for a security problem.

Include what an agent can do that it should not be able to (read another agent's rows,
change its identity, exhaust the fleet, tamper with the journal or audit log), the SQL or
API calls that reproduce it, and whether it needs the gateway, stock Postgres or the
SwarmPG kernel.

We aim to acknowledge reports within 3 business days and to agree a disclosure date with you.

## Scope

In scope: the gateway, `sql/schema.sql`, the kernel patches, and the isolation claims in
the README. Out of scope: the development defaults (`trust` authentication, the default
admin key, the Docker quickstart), which are documented as not for production.

## Supported versions

Pre-1.0: only the latest commit on `main` receives fixes.
