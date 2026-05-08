from __future__ import annotations

import logging
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

import structlog

from .auth import build_drive_credentials
from .config import PipelineConfig, Settings
from .drive_client import DriveClient
from .filters import ensure_dir, included
from .s3_client import S3Client
from .state import load_state, save_state
from .utils import DriveFileFingerprint, safe_join_s3_key


def configure_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO))
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.JSONRenderer(),
        ],
    )


log = structlog.get_logger(__name__)


def _drive_item_to_s3_key(cfg: PipelineConfig, rel_path: str) -> str:
    # Preserve folder structure unless user disables.
    if cfg.drive.preserve_path:
        return safe_join_s3_key(cfg.s3.prefix, rel_path)
    # Flatten: keep only basename
    return safe_join_s3_key(cfg.s3.prefix, os.path.basename(rel_path))


def run(config_path: str) -> dict:
    settings = Settings(config_path=config_path)
    cfg = settings.load_pipeline_config()
    configure_logging(cfg.behavior.log_level)

    ensure_dir(cfg.behavior.download_temp_dir)

    state = load_state(cfg.behavior.state_store_path)
    state.setdefault("files", {})

    creds = build_drive_credentials(cfg)
    drive = DriveClient(creds, page_size=cfg.drive.page_size)
    s3 = S3Client()

    # Collect items
    items = []
    for item in drive.list_folder_tree(cfg.drive.folder_id, recursive=cfg.drive.recursive):
        if item.mime_type == "application/vnd.google-apps.folder":
            continue
        if not included(item.name, cfg.drive.include_glob_patterns, cfg.drive.exclude_glob_patterns):
            continue
        items.append(item)
        if cfg.behavior.max_files and len(items) >= cfg.behavior.max_files:
            break

    log.info("discovered_files", count=len(items))

    def process_one(item):
        s3_key = _drive_item_to_s3_key(cfg, item.path)
        fp = DriveFileFingerprint(item.file_id, item.modified_time, item.size)
        prev = state["files"].get(fp.stable_key())
        if cfg.behavior.skip_if_unchanged and prev and prev.get("modified_time") == fp.modified_time and prev.get(
            "size"
        ) == fp.size:
            return {"status": "skipped_unchanged", "file_id": item.file_id, "s3_key": s3_key}

        if cfg.s3.keep_existing:
            head = s3.head_object(cfg.s3.bucket, s3_key)
            if head is not None:
                return {"status": "skipped_exists", "file_id": item.file_id, "s3_key": s3_key}

        if cfg.behavior.dry_run:
            return {"status": "dry_run", "file_id": item.file_id, "s3_key": s3_key}

        with tempfile.TemporaryDirectory(dir=cfg.behavior.download_temp_dir) as td:
            local_path = os.path.join(td, os.path.basename(item.path))
            drive.download_file(item.file_id, local_path, chunk_size=cfg.behavior.chunk_size_bytes)

            metadata = {
                f"{cfg.behavior.s3_metadata_prefix}file_id": item.file_id,
            }
            if item.modified_time:
                metadata[f"{cfg.behavior.s3_metadata_prefix}modified_time"] = item.modified_time
            if item.size is not None:
                metadata[f"{cfg.behavior.s3_metadata_prefix}size"] = str(item.size)

            res = s3.upload_file(
                local_path,
                cfg.s3.bucket,
                s3_key,
                metadata=metadata,
                server_side_encryption=cfg.s3.server_side_encryption,
                sse_kms_key_id=cfg.s3.server_side_encryption_kms_key_id,
            )

        state["files"][fp.stable_key()] = {
            "modified_time": fp.modified_time,
            "size": fp.size,
            "s3_key": s3_key,
            "etag": res.etag,
        }

        return {"status": "uploaded", "file_id": item.file_id, "s3_key": s3_key, "etag": res.etag}

    results = []
    failures = 0
    with ThreadPoolExecutor(max_workers=cfg.behavior.concurrency) as ex:
        futs = {ex.submit(process_one, it): it for it in items}
        for fut in as_completed(futs):
            it = futs[fut]
            try:
                r = fut.result()
                results.append(r)
                log.info("file_result", **r)
            except Exception as e:
                failures += 1
                log.exception("file_failed", file_id=it.file_id, path=it.path, error=str(e))
                if cfg.behavior.stop_on_error:
                    raise

    save_state(cfg.behavior.state_store_path, state)

    summary = {
        "discovered": len(items),
        "failures": failures,
        "results": results,
    }
    log.info("run_complete", **{k: v for k, v in summary.items() if k != "results"))
    return summary
