from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Iterator

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


@dataclass(frozen=True)
class DriveItem:
    file_id: str
    name: str
    mime_type: str
    modified_time: str | None
    size: int | None
    parents: list[str]
    path: str  # relative path from root folder, includes name


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class DriveClient:
    def __init__(self, credentials, *, page_size: int = 1000):
        self._svc = build("drive", "v3", credentials=credentials, cache_discovery=False)
        self._page_size = page_size

    def list_folder_tree(self, folder_id: str, *, recursive: bool = True) -> Iterator[DriveItem]:
        # BFS traversal to avoid deep recursion.
        queue: list[tuple[str, str]] = [(folder_id, "")]

        while queue:
            current_folder_id, current_path = queue.pop(0)
            for item in self._list_children(current_folder_id, current_path=current_path):
                yield item
                if recursive and item.mime_type == "application/vnd.google-apps.folder":
                    queue.append((item.file_id, item.path))

    def _list_children(self, folder_id: str, *, current_path: str) -> Iterable[DriveItem]:
        q = f"'{folder_id}' in parents and trashed=false"
        page_token = None

        while True:
            resp = (
                self._svc.files()
                .list(
                    q=q,
                    pageSize=self._page_size,
                    fields="nextPageToken, files(id,name,mimeType,modifiedTime,size,parents)",
                    pageToken=page_token,
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                )
                .execute()
            )

            for f in resp.get("files", []) or []:
                name = f.get("name")
                file_id = f.get("id")
                mime = f.get("mimeType")
                modified = f.get("modifiedTime")
                size = int(f["size"]) if f.get("size") is not None else None
                parents = f.get("parents") or []
                path = f"{current_path}/{name}" if current_path else name

                yield DriveItem(
                    file_id=file_id,
                    name=name,
                    mime_type=mime,
                    modified_time=modified,
                    size=size,
                    parents=parents,
                    path=path,
                )

            page_token = resp.get("nextPageToken")
            if not page_token:
                break

    def download_file(self, file_id: str, out_path: str, *, chunk_size: int = 8 * 1024 * 1024) -> None:
        request = self._svc.files().get_media(fileId=file_id, supportsAllDrives=True)
        with open(out_path, "wb") as f:
            downloader = MediaIoBaseDownload(f, request, chunksize=chunk_size)
            done = False
            while not done:
                _, done = downloader.next_chunk()

    def get_file_metadata(self, file_id: str) -> dict:
        return (
            self._svc.files()
            .get(
                fileId=file_id,
                fields="id,name,mimeType,modifiedTime,size,md5Checksum",
                supportsAllDrives=True,
            )
            .execute()
        )
