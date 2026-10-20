**What this changes and why**

- [ ] I have signed the [CLA](https://github.com/AlmarionAI/swarmpg/blob/main/CLA.md) (the CLA check will ask on your first pull request)
- [ ] `make test` and `make e2e` pass; `make e2e KERNEL=1` too if the kernel or schema changed
- [ ] A change to the isolation boundary comes with a probe in `loadtest/` that fails without it
- [ ] Documented commands still run (`scripts/check-docs.sh`, run by CI)
