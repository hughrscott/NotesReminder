import argparse
import os
import boto3
from dotenv import load_dotenv

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
    s3 = boto3.client("s3", region_name=args.region)
    s3.upload_file(args.db, args.bucket, args.key)
    print(f"Uploaded {args.db} to s3://{args.bucket}/{args.key}")


if __name__ == "__main__":
    main()
