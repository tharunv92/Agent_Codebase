from __future__ import annotations

import json
import logging
import os
from dataclasses import asdict

from google.oauth2 import service_account
from google.oauth2.credentials import Credentials as UserCredentials

from .config import PipelineConfig


log = logging.getLogger(__name__)


def build_drive_credentials(cfg: PipelineConfig):
    ga = cfg.google_auth
    if ga.mode == "service_account":
        if not ga.service_account_file:
            raise ValueError("google_auth.service_account_file is required for service_account mode")
        return service_account.Credentials.from_service_account_file(
            ga.service_account_file, scopes=ga.scopes
        )

    if ga.mode == "oauth":
        if not ga.oauth_token_file:
            raise ValueError("google_auth.oauth_token_file is required for oauth mode")
        with open(ga.oauth_token_file, "r", encoding="utf-8") as f:
            token = json.load(f)
        # expected keys: client_id, client_secret, refresh_token, token_uri
        return UserCredentials(
            None,
            refresh_token=token.get("refresh_token"),
            token_uri=token.get("token_uri", "https://oauth2.googleapis.com/token"),
            client_id=token.get("client_id"),
            client_secret=token.get("client_secret"),
            scopes=ga.scopes,
        )

    raise ValueError(f"Unsupported google_auth.mode: {ga.mode}")
