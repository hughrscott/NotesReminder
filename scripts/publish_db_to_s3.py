import argparse
import os
from datetime import datetime
from pathlib import Path
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from scripts.db_guard import db_meta

load_dotenv()


def parse_args():
    parser = argparse.ArgumentParser(description="Upload reminders.db to S3.")
    parser.add_argument("--db", default="reminders.db")
    parser.add_argument("--bucket", default=os.getenv("REMINDERS_S3_BUCKET", "notesreminder-db"))
    parser.add_argument("--key", default=os.getenv("REMINDERS_S3_KEY", "reminders.db"))
    parser.add_argument("--region", default=os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
    return parser.parse_args()


def main():
    args = parse_args()
    if not os.path.exists(args.db):
        raise SystemExit(f"DB not found: {args.db}")
    meta = db_meta(Path(args.db))
    if meta.get("table_count") in (None, 0) or meta.get("integrity_check") != "ok":
        raise SystemExit(f"Refusing to upload invalid DB: {meta}")
    s3 = boto3.client("s3", region_name=args.region)
    stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    backup_key = f"backups/{Path(args.key).name}.before-publish-{stamp}.bak"
    try:
        s3.copy_object(
            Bucket=args.bucket,
            CopySource={"Bucket": args.bucket, "Key": args.key},
            Key=backup_key,
        )
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in {"NoSuchKey", "404", "NotFound"}:
            raise SystemExit(
                f"Refusing to upload: no existing s3://{args.bucket}/{args.key} to back up."
            ) from exc
        raise
    s3.upload_file(args.db, args.bucket, args.key)
    print(f"Backed up current DB to s3://{args.bucket}/{backup_key}")
    print(f"Uploaded {args.db} to s3://{args.bucket}/{args.key}")


if __name__ == "__main__":
    main()
