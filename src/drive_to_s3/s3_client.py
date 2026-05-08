from __future__ import annotations

from dataclasses import dataclass

import boto3


@dataclass
class S3UploadResult:
    bucket: str
    key: str
    etag: str | None


class S3Client:
    def __init__(self):
        self._s3 = boto3.client("s3")

    def head_object(self, bucket: str, key: str) -> dict | None:
        try:
            return self._s3.head_object(Bucket=bucket, Key=key)
        except self._s3.exceptions.NoSuchKey:  # pragma: no cover
            return None
        except Exception as e:
            # boto3 raises ClientError for 404 typically
            from botocore.exceptions import ClientError

            if isinstance(e, ClientError) and e.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404:
                return None
            raise

    def upload_file(
        self,
        file_path: str,
        bucket: str,
        key: str,
        *,
        metadata: dict[str, str] | None = None,
        server_side_encryption: str | None = None,
        sse_kms_key_id: str | None = None,
    ) -> S3UploadResult:
        extra_args: dict = {}
        if metadata:
            extra_args["Metadata"] = metadata
        if server_side_encryption:
            extra_args["ServerSideEncryption"] = server_side_encryption
        if sse_kms_key_id:
            extra_args["SSEKMSKeyId"] = sse_kms_key_id

        self._s3.upload_file(file_path, bucket, key, ExtraArgs=extra_args or None)
        head = self._s3.head_object(Bucket=bucket, Key=key)
        return S3UploadResult(bucket=bucket, key=key, etag=head.get("ETag"))
