from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    bundle = root / "services" / "local-agent" / "models"
    manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    failures: list[str] = []
    for artifact in manifest.get("artifacts", []):
        relative_path = artifact.get("relative_path")
        expected = str(artifact.get("sha256") or "").removeprefix("sha256:")
        path = bundle / str(relative_path)
        if not path.is_file():
            failures.append(f"missing: {relative_path}")
            continue
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != expected:
            failures.append(f"checksum mismatch: {relative_path}")
    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    print(f"verified {len(manifest.get('artifacts', []))} local Agent artifacts")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
