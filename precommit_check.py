"""Pre-commit style check: refuse to pass if any text file under the project contains something that looks like a credential.

    python precommit_check.py            # scans the project (skips logs/, __pycache__, venvs, binary files); exit 1 on a finding
    python precommit_check.py FILE ...   # scans only the given files (use as a git pre-commit hook: pass the staged files)

It prints file:line and the KIND of finding, never the matched text. This project keeps its keys in the Windows Credential Manager (service
"updown5m"); a hit here means a value that should be there ended up in a file."""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SKIP_DIRS = {"logs", "__pycache__", ".git", "data_archive", "node_modules"}
SKIP_SUFFIX = {".gz", ".png", ".jpg", ".zip", ".pyc", ".bin", ".parquet", ".csv", ".jsonl", ".pdf"}
PATTERNS = [
    ("private key block", re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")),
    ("64-hex private key assigned to a key-like name", re.compile(r"(?i)(priv\w*|pk|wallet_key|signer_key)\s*[:=]\s*['\"]?(0x)?[0-9a-f]{64}\b")),
    ("secret-like name assigned a literal", re.compile(r"(?i)(api_?secret|passphrase|app_?password|password|private_?key|secret)\w*\s*[:=]\s*['\"][A-Za-z0-9+/=_\-]{12,}['\"]")),
    ("gmail app password shape after a password-like word", re.compile(r"(?i)(password|passwd|pwd|app.?pass)\w*\W{0,6}[a-z]{4} [a-z]{4} [a-z]{4} [a-z]{4}\b")),
    ("bearer / authorization header with a value", re.compile(r"(?i)authorization['\"]?\s*[:=]\s*['\"]bearer\s+[A-Za-z0-9._\-]{16,}")),
    ("AWS access key id", re.compile(r"\bAKIA[0-9A-Z]{16}\b")),
]
# placeholders that tests and docs legitimately contain
ALLOW = re.compile(r"(?i)(REDACTED|example|placeholder|xxxxxxxx|your[_-]|<.*>|0x0{40})")


def scan_text(text, name="<text>"):
    out = []
    for i, line in enumerate(text.splitlines(), 1):
        for kind, rx in PATTERNS:
            m = rx.search(line)
            if m and not ALLOW.search(line):
                out.append((name, i, kind))
    return out


def iter_files(paths):
    if paths:
        yield from (Path(p) for p in paths)
        return
    for p in ROOT.rglob("*"):
        if p.is_file() and not (set(p.relative_to(ROOT).parts) & SKIP_DIRS) and p.suffix.lower() not in SKIP_SUFFIX \
                and not any(part.startswith(".venv") for part in p.parts):
            yield p


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    hits = []
    for p in iter_files(argv):
        if p.name == Path(__file__).name or p.suffix.lower() in SKIP_SUFFIX:
            continue
        try:
            text = p.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        hits += scan_text(text, str(p))
    for name, line, kind in hits:
        print(f"{name}:{line}: {kind}")
    print("precommit_check: " + ("FAILED - remove the value(s) above" if hits else "clean"))
    return 1 if hits else 0


if __name__ == "__main__":
    raise SystemExit(main())
