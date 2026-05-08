from __future__ import annotations

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict



class GoogleAuthConfig(BaseModel):
    """Config for Google Drive API authentication.

    Supported auth modes:
    - service_account: load credentials from a service account JSON file
    - oauth: use user credentials from a refresh token JSON

    Note: No secrets should be hardcoded in the config. Point to paths or use env variables.
    """

    mode: str = Field(default="service_account", description="service_account | oauth")
    service_account_file: str | None = Field(default=None)
    oauth_token_file: str | None = Field(default=None)
    scopes: list[str] = Field(
        default_factory=lambda: ["https://www.googleapis.com/auth/drive.readonly"]
    )



class DriveSourceConfig(BaseModel):
    """What to ingest from Drive."""

    folder_id: str = Field(description="Google Drive folder ID to list/download")
    include_glob_patterns: list[str] = Field(default_factory=lambda: ["*"])
    exclude_glob_patterns: list[str] = Field(default_factory=list)
    recursive: bool = Field(default=True)
    preserve_path: bool = Field(
        default=True, description="Preserve Drive path under the s3 prefix"
    )

    # Huge folders: use page size to control API page size.
    page_size: int = Field(default=1000, ge=1, le=1000)



class S3SinkConfig(BaseModel):
    """Where to write in S3."""

    bucket: str = Field(description="S3 bucket name")
    prefix: str = Field(default="", description="S3 key prefix (folder)")
    keep_existing: bool = Field(
        default=True, description="If true, don't overwrite existing objects"
    )
    server_side_encryption: str | None = Field(
        default=None, description="Optional: AES256 | aws:kms: etc."
    )
    server_side_encryption_kms_key_id: str | None = Field(default=None)



class BehaviorConfig(BaseModel):
    """Pipeline behavior and safety switches."""

    concurrency: int = Field(default=4, ge=1, le=32)
    chunk_size_bytes: int = Field(default=8 * 1024 * 1024 , ge=1)  # 8MB
    download_temp_dir: str = Field(default="/tmp/drive_to_s3")
    state_store_path: str = Field(default="./state/drive_to_s3-state.json")
    max_files: int | None = Field(default=None, description="Optional: limit files")
    dry_run: bool = Field(default=False)

    # Failure tolerance
    stop_on_error: bool = Field(default=False)

    # Observability
    log_level: str = Field(default="INFO")

    # Idempotence
    skip_if_unchanged: bool = Field(
        default=True,
        description="Skip upload if Drive modifiedTime and size unchanged",
    )

    s3_metadata_prefix: str = Field(
        default="drive_", description="Metadata key prefix for S3 objects"
    )



class PipelineConfig(BaseModel):
    google_auth: GoogleAuthConfig
    drive: DriveSourceConfig
    s3: S3SinkConfig
    behavior: BehaviorConfig = Field(default_factory=BehaviorConfig)



class Settings(BaseSettings):
    """Load settings from env and/or a config file.

    Flow:
    - default config path is `config/example.yml`
    - runtime override via env `DRIVE_TO_S3_CONFIG` or CLI `--config`

    Secrets should be provided as env variables or file paths pointing to secret storage.
    """

    config_path: str = Field(default="config/example.yml", alias="DRIVE_TO_S3_CONFIG")

    model_config = SettingsConfigDict(env_file_encoding="utf-8", extra="ignore")

    def load_pipeline_config(self) -> PipelineConfig:
        import yaml

        with open(self.config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return PipelineConfig.model_validate(data)
