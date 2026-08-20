# Local Agent

The Local Agent is the read-only channel profile data-processing module used by
the full and incremental Agent queues. It runs as a Python child process inside
the qybullmq image; it is not a separate HTTP service and does not modify crawler
source rows.

```text
src/      inference, evidence, feature, and output-contract code
tests/    unit and regression tests
scripts/  read-only export tooling
models/   versioned model artifacts and manifest
```

The model artifacts are tracked with Git LFS. Validate their checksums from the
repository root:

```bash
python3 scripts/verify_model_bundle.py
```

Run the Local Agent tests with:

```bash
cd services/local-agent
python3 -m pytest -q
```
