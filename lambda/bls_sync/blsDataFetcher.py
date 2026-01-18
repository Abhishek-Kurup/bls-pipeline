import boto3
import urllib.request
from html.parser import HTMLParser


class LinkParser(HTMLParser):
    """Simple HTML parser to extract href links."""

    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for attr, value in attrs:
                if attr == "href":
                    self.links.append(value)


def lambda_handler(event, context):
    s3_bucket = event.get("S3_BUCKET", None)
    bls_config = event.get("CONFIG", None)
    if not bls_config:
        raise Exception("BLS Configs not passed")

    base_url = bls_config.get("url", None)
    headers = bls_config.get("header", None)
    s3_prefix = bls_config.get("s3_prefix", None)


    # --- Fetch source directory listing---
    req = urllib.request.Request(
        base_url,
        headers=headers
    )
    with urllib.request.urlopen(req) as response:
        html = response.read().decode("utf-8")

    parser = LinkParser()
    parser.feed(html)
    source_files = [f.split("/")[-1] for f in parser.links if
                    f and isinstance(f, str) and f.strip() != "" and not f.endswith("/")]
    print(f"No of files in source: {len(source_files)}")

    # --- Check existing files in S3 ---
    s3 = boto3.client("s3")
    existing_files = {}
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix):
        for obj in page.get("Contents", []):
            filename = obj["Key"].replace(s3_prefix, "")
            if not filename:
                continue
            existing_files[filename] = obj["Size"]

    print(f"S3 file count: {len(existing_files)}")

    uploaded = []
    skipped = []
    archived = []
    updated = []
    for filename in existing_files:
        if filename not in source_files:
            source_key = f"{s3_prefix}{filename}"
            archive_key = f"{s3_prefix}archive/{filename}"

            s3.copy_object(
                Bucket=s3_bucket,
                CopySource={"Bucket": s3_bucket, "Key": source_key},
                Key=archive_key
            )

            s3.delete_object(
                Bucket=s3_bucket,
                Key=source_key
            )

            archived.append(filename)

    for filename in source_files:
        file_url = base_url + filename

        try:
            # HEAD request to get source file size
            req_head = urllib.request.Request(
                file_url,
                headers=headers,
                method="HEAD"
            )
            with urllib.request.urlopen(req_head) as resp:
                source_size = int(resp.headers.get("Content-Length", 0))

            s3_size = existing_files.get(filename)

            # Decide action
            if s3_size is not None and s3_size == source_size:
                skipped.append(filename)
                continue

            # Download file only if new or updated
            req_file = urllib.request.Request(file_url, headers=headers)
            with urllib.request.urlopen(req_file) as f:
                data = f.read()

            s3.put_object(
                Bucket=s3_bucket,
                Key=f"{s3_prefix}{filename}",
                Body=data
            )

            if s3_size is None:
                uploaded.append(filename)
            else:
                updated.append(filename)

        except Exception as e:
            print(f"Error processing {filename}: {e}")

    return {
        "statusCode": 200,
        "body": {
            "uploaded": uploaded,
            "updated": updated,
            "skipped": skipped,
            "archived": archived
        }
    }