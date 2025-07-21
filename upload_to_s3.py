#!/usr/bin/env python3
"""Upload local nursing home data files to S3.

Example:
    python upload_to_s3.py -b my-bucket -p raw/nursing_homes/ --profile default
"""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import logging
import sys
from pathlib import Path
from typing import Iterable, List, Optional

import boto3
from boto3.s3.transfer import S3Transfer, TransferConfig
from botocore.exceptions import ClientError, NoCredentialsError

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover - tqdm optional
    tqdm = None  # type: ignore

CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB


def compute_sha256(path: Path) -> str:
    """Compute SHA256 for a file in streaming fashion."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
            h.update(chunk)
    return h.hexdigest()


def should_include(
    rel_path: str,
    includes: List[str],
    excludes: List[str],
    explicit: bool = False,
) -> bool:
    """Determine if a relative path should be included."""
    # Hidden path detection
    parts = Path(rel_path).parts
    hidden = any(p.startswith(".") for p in parts)
    if hidden and not explicit:
        return False

    if includes:
        if not any(fnmatch.fnmatch(rel_path, pat) for pat in includes):
            return False

    if excludes:
        if any(fnmatch.fnmatch(rel_path, pat) for pat in excludes):
            return False

    return True


def collect_files(
    source: Path,
    includes: List[str],
    excludes: List[str],
    max_size_mb: Optional[int] = None,
) -> List[Path]:
    """Collect files under source applying include/exclude rules."""
    files: List[Path] = []
    max_bytes = max_size_mb * 1024 * 1024 if max_size_mb else None
    for path in source.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(source).as_posix()
        explicit = includes and any(fnmatch.fnmatch(rel, pat) for pat in includes)
        if not should_include(rel, includes, excludes, explicit):
            continue
        if max_bytes and path.stat().st_size > max_bytes:
            logging.warning("SKIP (size>max) %s", rel)
            continue
        files.append(path)
    return files


def head_object(s3, bucket: str, key: str) -> Optional[dict]:
    try:
        return s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:  # pragma: no cover - network
        code = e.response.get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise


def upload_file(
    transfer: S3Transfer,
    bucket: str,
    key: str,
    path: Path,
    metadata: dict,
    storage_class: Optional[str],
    show_progress: bool,
) -> None:
    extra_args = {"Metadata": metadata}
    if storage_class:
        extra_args["StorageClass"] = storage_class

    if show_progress and tqdm:
        with tqdm(total=path.stat().st_size, unit="B", unit_scale=True, desc=key, leave=False) as bar:
            def _cb(bytes_amount: int) -> None:
                bar.update(bytes_amount)

            transfer.upload_file(str(path), bucket, key, extra_args=extra_args, callback=_cb)
    else:
        transfer.upload_file(str(path), bucket, key, extra_args=extra_args)


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Upload files to S3")
    parser.add_argument("--source", "-s", default="./Nursing_Homes_data", help="source directory")
    parser.add_argument("--bucket", "-b", required=True, help="S3 bucket")
    parser.add_argument("--prefix", "-p", default="", help="key prefix")
    parser.add_argument("--region", "-r", help="AWS region")
    parser.add_argument("--profile", help="AWS profile")
    parser.add_argument("--concurrency", "-c", type=int, default=8, help="parallel workers")
    parser.add_argument("--dry-run", action="store_true", help="list only")
    parser.add_argument("--skip-existing", action="store_true")
    parser.add_argument("--manifest", help="write SHA256 manifest")
    parser.add_argument("--verify", action="store_true", help="verify after upload")
    parser.add_argument("--exclude", action="append", default=[], help="exclude glob")
    parser.add_argument("--include", action="append", default=[], help="include glob")
    parser.add_argument("--max-size-mb", type=int)
    parser.add_argument("--multipart-threshold-mb", type=int, default=8)
    parser.add_argument("--progress", action="store_true")
    parser.add_argument("--fail-fast", action="store_true")
    parser.add_argument("--storage-class")
    parser.add_argument("--verbose", action="store_true")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(message)s")

    source = Path(args.source).expanduser()
    if not source.is_dir():
        logging.error("Source directory not found: %s", source)
        return 1

    session_kwargs = {}
    if args.profile:
        session_kwargs["profile_name"] = args.profile
    if args.region:
        session_kwargs["region_name"] = args.region
    session = boto3.Session(**session_kwargs)

    if not session.get_credentials():  # pragma: no cover - env dependent
        logging.error("AWS credentials not found. Configure credentials via profile or env vars.")
        return 1

    s3 = session.client("s3")
    config = TransferConfig(
        max_concurrency=args.concurrency,
        multipart_threshold=args.multipart_threshold_mb * 1024 * 1024,
    )
    transfer = S3Transfer(client=s3, config=config)

    files = collect_files(source, args.include, args.exclude, args.max_size_mb)

    total = len(files)
    uploaded = 0
    skipped = 0
    errors = 0
    bytes_uploaded = 0

    manifest_file = open(args.manifest, "a") if args.manifest else None

    for path in files:
        rel = path.relative_to(source).as_posix()
        key = f"{args.prefix}{rel}" if args.prefix else rel
        size = path.stat().st_size
        mtime = int(path.stat().st_mtime)
        sha = compute_sha256(path)
        metadata = {"sha256": sha, "src-mtime": str(mtime), "src-bytes": str(size)}
        try:
            if args.skip_existing:
                head = head_object(s3, args.bucket, key)
                if head and head.get("ContentLength") == size and head.get("Metadata", {}).get("sha256") == sha:
                    logging.info("SKIP %d %s %s", size, key, sha[:10])
                    if manifest_file:
                        manifest_file.write(f"{sha}  {key}\n")
                    skipped += 1
                    continue

            logging.info("UPLOAD %d %s %s", size, key, sha[:10])
            if not args.dry_run:
                upload_file(transfer, args.bucket, key, path, metadata, args.storage_class, args.progress)
                bytes_uploaded += size
                if args.verify:
                    head = head_object(s3, args.bucket, key)
                    if not head or head.get("ContentLength") != size or head.get("Metadata", {}).get("sha256") != sha:
                        raise RuntimeError("verification failed")
            if manifest_file:
                manifest_file.write(f"{sha}  {key}\n")
            uploaded += 1
        except (ClientError, OSError, RuntimeError) as e:  # pragma: no cover - network
            logging.error("ERROR %s: %s", key, e)
            errors += 1
            if args.fail_fast:
                break

    if manifest_file:
        manifest_file.close()

    logging.info("Scanned: %d", total)
    logging.info("Uploaded: %d", uploaded)
    logging.info("Skipped: %d", skipped)
    logging.info("Errors: %d", errors)
    logging.info("Bytes uploaded: %.2f MB", bytes_uploaded / (1024 * 1024))

    return 1 if errors else 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
