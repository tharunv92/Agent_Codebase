from __future__ import annotations

import os
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator


#c Only non-sensitive config is stored here. Credentials should be provided
#  via environment vars / secret manager mounts.