"""Safe entrypoint for manifest-scoped 1C updates."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .one_c_receiver_runtime.receiver import ReceiverError, run_from_paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=os.getenv("UGKOREA_ONE_C_MANIFEST"))
    parser.add_argument("--source", type=Path, default=os.getenv("UGKOREA_ONE_C_EXPORT_DIR"))
    parser.add_argument("--database-url", default=os.getenv("UGKOREA_ONE_C_DATABASE_URL"))
    args = parser.parse_args()
    if args.manifest is None or args.source is None or not args.database_url:
        parser.error(
            "manifest, source and database URL must be supplied explicitly "
            "by arguments or UGKOREA_ONE_C_* environment variables"
        )
    return args


def main() -> int:
    args = parse_args()
    try:
        result = run_from_paths(
            database_url=args.database_url,
            source_dir=args.source,
            manifest_path=args.manifest,
        )
    except ReceiverError as error:
        print(json.dumps({"status": "rejected", "error": str(error)}, ensure_ascii=False))
        return 1
    except Exception as error:
        print(
            json.dumps(
                {"status": "failed", "error_code": type(error).__name__},
                ensure_ascii=False,
            )
        )
        return 1
    print(
        json.dumps(
            {
                "run_id": str(result.run_id),
                "status": result.status,
                "applied_contracts": result.applied_contracts,
                "applied_rows": result.applied_rows,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
