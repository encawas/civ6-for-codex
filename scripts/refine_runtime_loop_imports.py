from pathlib import Path

path = Path("src/civ6_workflow/domain/state_delta.py")
text = path.read_text(encoding="utf-8")
lines = text.splitlines()
imports = {
    "import json",
    "from enum import StrEnum",
    "from hashlib import sha256",
}
if not imports.issubset(lines):
    missing = sorted(imports - set(lines))
    raise SystemExit(f"state_delta standard-library imports are missing: {missing}")
lines = [line for line in lines if line not in imports]
future_index = lines.index("from __future__ import annotations")
insert_at = future_index + 1
while insert_at < len(lines) and not lines[insert_at].strip():
    insert_at += 1
lines[insert_at:insert_at] = [
    "import json",
    "from enum import StrEnum",
    "from hashlib import sha256",
    "",
]
path.write_text("\n".join(lines) + "\n", encoding="utf-8")
