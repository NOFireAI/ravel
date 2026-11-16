#!/usr/bin/env python3
"""Minimal SigV4 S3 client for the released-binary fence drill, standard
library only: create a bucket, list every key of a bucket with its
LastModified, and read the 16-byte trailer of a data object.

usage:
  drill_s3.py mb BUCKET
  drill_s3.py ls BUCKET [PREFIX]    # one line per key: "<key>\t<LastModified>\t<size>"
  drill_s3.py trailer BUCKET KEY    # "<key>\t<magic>\t<version>\t<signal>"
  drill_s3.py trailer-file PATH     # the same, for a local file

Reads S3_ENDPOINT (http://host:port), AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
and AWS_REGION (default us-east-1). Path-style requests only. A listing that
fails or is truncated without a continuation token exits non-zero, so a
partial listing is never mistaken for a complete one.
"""

import datetime
import hashlib
import hmac
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"


def sign(key, msg):
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()


def request(method, bucket, query, body=b"", key="", extra_headers=None):
    endpoint = os.environ["S3_ENDPOINT"].rstrip("/")
    access = os.environ["AWS_ACCESS_KEY_ID"]
    secret = os.environ["AWS_SECRET_ACCESS_KEY"]
    region = os.environ.get("AWS_REGION", "us-east-1")
    host = urllib.parse.urlparse(endpoint).netloc
    now = datetime.datetime.now(datetime.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    day = now.strftime("%Y%m%d")
    path = "/" + bucket + ("/" + urllib.parse.quote(key, safe="/-_.~") if key else "")
    canonical_query = "&".join(
        "%s=%s"
        % (urllib.parse.quote(k, safe="-_.~"), urllib.parse.quote(v, safe="-_.~"))
        for k, v in sorted(query.items())
    )
    payload_hash = hashlib.sha256(body).hexdigest()
    headers = {"host": host, "x-amz-content-sha256": payload_hash, "x-amz-date": amz_date}
    signed = ";".join(sorted(headers))
    canonical = "\n".join(
        [
            method,
            path,
            canonical_query,
            "".join("%s:%s\n" % (k, headers[k]) for k in sorted(headers)),
            signed,
            payload_hash,
        ]
    )
    scope = "%s/%s/s3/aws4_request" % (day, region)
    to_sign = "\n".join(
        ["AWS4-HMAC-SHA256", amz_date, scope, hashlib.sha256(canonical.encode()).hexdigest()]
    )
    k = sign(("AWS4" + secret).encode(), day)
    k = sign(k, region)
    k = sign(k, "s3")
    k = sign(k, "aws4_request")
    signature = hmac.new(k, to_sign.encode(), hashlib.sha256).hexdigest()
    auth = "AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s" % (
        access,
        scope,
        signed,
        signature,
    )
    url = endpoint + path + ("?" + canonical_query if canonical_query else "")
    req = urllib.request.Request(url, data=body if method == "PUT" else None, method=method)
    for name, value in headers.items():
        if name != "host":
            req.add_header(name, value)
    req.add_header("Authorization", auth)
    for name, value in (extra_headers or {}).items():
        req.add_header(name, value)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read()


def make_bucket(bucket):
    request("PUT", bucket, {})


def list_bucket(bucket, prefix):
    token = None
    while True:
        query = {"list-type": "2", "max-keys": "1000"}
        if prefix:
            query["prefix"] = prefix
        if token:
            query["continuation-token"] = token
        root = ET.fromstring(request("GET", bucket, query))
        for item in root.findall(NS + "Contents"):
            print(
                "%s\t%s\t%s"
                % (
                    item.findtext(NS + "Key"),
                    item.findtext(NS + "LastModified"),
                    item.findtext(NS + "Size"),
                )
            )
        if root.findtext(NS + "IsTruncated") != "true":
            return
        token = root.findtext(NS + "NextContinuationToken")
        if not token:
            raise SystemExit("listing truncated without a continuation token")


def trailer_line(key, tail):
    """The last 16 bytes of an RSEG/RLOG/RSPAN object: footer_len u32,
    footer_crc32c u32, version u16 LE, signal u8, reserved u8, magic [u8; 4]
    (docs/segment-format.md, docs/log-segment-format.md,
    docs/span-segment-format.md)."""
    if len(tail) != 16:
        raise SystemExit("trailer of %s: %d bytes, want 16" % (key, len(tail)))
    magic = tail[12:16].decode("ascii", "replace")
    version = int.from_bytes(tail[8:10], "little")
    return "%s\t%s\t%d\t%d" % (key, magic, version, tail[10])


def trailer(bucket, key):
    tail = request("GET", bucket, {}, key=key, extra_headers={"Range": "bytes=-16"})
    print(trailer_line(key, tail))


def main(argv):
    try:
        if len(argv) == 3 and argv[1] == "mb":
            make_bucket(argv[2])
            return 0
        if len(argv) in (3, 4) and argv[1] == "ls":
            list_bucket(argv[2], argv[3] if len(argv) == 4 else "")
            return 0
        if len(argv) == 4 and argv[1] == "trailer":
            trailer(argv[2], argv[3])
            return 0
        if len(argv) == 3 and argv[1] == "trailer-file":
            with open(argv[2], "rb") as f:
                print(trailer_line(argv[2], f.read()[-16:]))
            return 0
    except urllib.error.HTTPError as e:
        sys.stderr.write("drill_s3: %s %s\n" % (e.code, e.read()[:300]))
        return 1
    sys.stderr.write(__doc__)
    return 64


if __name__ == "__main__":
    sys.exit(main(sys.argv))
