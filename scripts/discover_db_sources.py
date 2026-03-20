#!/usr/bin/env python3
import argparse
import os
from collections import defaultdict

import boto3
from dotenv import load_dotenv


def parse_args():
    parser = argparse.ArgumentParser(
        description="Discover candidate S3 DB snapshots (versions + backup keys)."
    )
    parser.add_argument(
        "--bucket", default=os.getenv("REMINDERS_S3_BUCKET", "notesreminder-db")
    )
    parser.add_argument("--key", default=os.getenv("REMINDERS_S3_KEY", "reminders.db"))
    parser.add_argument(
        "--prefixes",
        nargs="*",
        default=["reminders", "backup", "archive", "db"],
        help="Additional key prefixes to inspect via list_objects_v2.",
    )
    parser.add_argument("--region", default=os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
    parser.add_argument("--limit", type=int, default=50)
    return parser.parse_args()


def list_versions(s3, bucket: str, key: str):
    paginator = s3.get_paginator("list_object_versions")
    out = []
    for page in paginator.paginate(Bucket=bucket, Prefix=key):
        for version in page.get("Versions", []):
            if version.get("Key") == key:
                out.append(version)
    return out


def list_candidate_keys(s3, bucket: str, prefixes):
    out = []
    for pref in prefixes:
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=pref, MaxKeys=1000)
        for obj in resp.get("Contents", []):
            key = obj.get("Key", "")
            if key.endswith(".db") or "reminders" in key.lower():
                out.append(obj)
    dedup = {}
    for row in out:
        dedup[row["Key"]] = row
    return list(dedup.values())


def main():
    load_dotenv()
    args = parse_args()
    s3 = boto3.client("s3", region_name=args.region)

    versions = list_versions(s3, args.bucket, args.key)
    candidates = list_candidate_keys(s3, args.bucket, args.prefixes)

    ranked = []
    for v in versions:
        ranked.append(
            {
                "type": "version",
                "key": v["Key"],
                "version_id": v["VersionId"],
                "size": v["Size"],
                "last_modified": v["LastModified"],
            }
        )
    for c in candidates:
        ranked.append(
            {
                "type": "object",
                "key": c["Key"],
                "version_id": "",
                "size": c["Size"],
                "last_modified": c["LastModified"],
            }
        )

    ranked.sort(
        key=lambda x: (
            x["last_modified"],
            x["size"],
        ),
        reverse=True,
    )

    print(f"bucket={args.bucket}")
    print(f"primary_key={args.key}")
    print(f"version_count={len(versions)} candidate_key_count={len(candidates)}")
    print("")
    grouped = defaultdict(int)
    for row in ranked[: args.limit]:
        grouped[row["type"]] += 1
        lm = row["last_modified"].isoformat(timespec="seconds")
        size_mb = row["size"] / (1024 * 1024)
        print(
            f"{row['type']:7} key={row['key']} version_id={row['version_id'] or '-'} "
            f"size_mb={size_mb:.2f} last_modified={lm}"
        )
        if row["type"] == "version":
            print(
                "  download: "
                f"aws s3api get-object --bucket {args.bucket} --key {row['key']} "
                f"--version-id '{row['version_id']}' recovered_{row['version_id']}.db"
            )
        else:
            filename = row["key"].split("/")[-1]
            print(
                "  download: "
                f"aws s3 cp s3://{args.bucket}/{row['key']} recovered_{filename}"
            )

    print("")
    print(f"shown={sum(grouped.values())} (limit={args.limit})")


if __name__ == "__main__":
    main()
