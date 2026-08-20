# Local Agent Model Bundle

The runtime bundle is stored in `services/local-agent/models`. `manifest.json` records
artifact paths, SHA-256 digests, training provenance, taxonomy identity, and
metrics. Run this before building:

```bash
python3 scripts/verify_model_bundle.py
```

The artifacts exceed GitHub's normal per-file limit and are tracked through Git
LFS by `.gitattributes`. A clone without `git lfs pull` will not produce a valid
qybullmq image.
