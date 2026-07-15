from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path


class CredentialError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ApiCredential:
    value: str
    source: str


def resolve_api_credential(
    env_name: str,
    file_path: str | Path | None = None,
) -> ApiCredential | None:
    value = os.environ.get(env_name, "").strip()
    if value:
        return ApiCredential(value=value, source=f"environment:{env_name}")

    if file_path is None or not str(file_path).strip():
        return None

    path = Path(os.path.expandvars(str(file_path))).expanduser()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise CredentialError(f"API credential file was not found: {path}") from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise CredentialError(f"API credential file could not be read: {path}") from exc

    if not isinstance(payload, dict):
        raise CredentialError(f"API credential file must contain a JSON object: {path}")
    value = payload.get(env_name)
    if not isinstance(value, str) or not value.strip():
        raise CredentialError(
            f"API credential file does not contain a non-empty {env_name}: {path}"
        )
    return ApiCredential(value=value.strip(), source=f"file:{path}")
