from pathlib import Path

path = Path("src/civ6_workflow/domain/state_delta.py")
text = path.read_text(encoding="utf-8")
old = '''from enum import StrEnum
from hashlib import sha256
import json
'''
new = '''import json
from enum import StrEnum
from hashlib import sha256
'''
if text.count(old) != 1:
    raise SystemExit("state_delta standard-library import block is not unique")
path.write_text(text.replace(old, new, 1), encoding="utf-8")
