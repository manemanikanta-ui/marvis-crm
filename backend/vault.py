"""
vault.py — MARVIS Memory Layer (Phase 0)
=========================================
Server-side interface between MARVIS CRM and the Obsidian vault at
C:\\Users\\HP\\PycharmProjects\\PythonProject\\MARVIS_Vault\\

Design rules honoured:
  - Pure stdlib. No new dependencies.
  - Never raises on missing notes (returns None / empty) — the CRM must
    never crash because a vault note doesn't exist yet.
  - Patterns.md reads are mtime-cached: hot path (enrichment.py calls
    get_active_patterns on every generation) costs one os.stat, not a read.
  - Path traversal guarded: rel paths cannot escape the vault root.
  - Writes are atomic (tmp file + os.replace) so Obsidian never sees a
    half-written note.

Live-wire endpoints this enables (add to main.py):
  GET /api/vault/manifest   -> manifest()        (feeds HUD bubble view)
  GET /api/vault/note?rel=  -> read_note(rel)    (HUD panels)
"""

from __future__ import annotations

import os
import re
import tempfile
from datetime import datetime, date
from pathlib import Path
from typing import Optional

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------
VAULT_ROOT = Path(
    os.environ.get(
        "MARVIS_VAULT_PATH",
        r"C:\Users\HP\PycharmProjects\PythonProject\MARVIS_Vault",
    )
)

# folders the manifest exposes to the HUD (order = display order)
MANIFEST_FOLDERS = [
    "brain", "projects", "campaigns", "clients",
    "research", "daily", "outcomes",
]

# --------------------------------------------------------------------------
# Internal helpers
# --------------------------------------------------------------------------

def _resolve(rel: str) -> Optional[Path]:
    """Resolve a vault-relative path safely. Returns None if it escapes root."""
    try:
        p = (VAULT_ROOT / rel).resolve()
        p.relative_to(VAULT_ROOT.resolve())   # raises ValueError on escape
        return p
    except (ValueError, OSError):
        return None


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
        os.replace(tmp, str(path))
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# --------------------------------------------------------------------------
# Core read / write API
# --------------------------------------------------------------------------

def read_note(rel: str) -> Optional[str]:
    """Read a vault note by relative path. None if missing or unsafe."""
    p = _resolve(rel)
    if p is None or not p.is_file():
        return None
    try:
        return p.read_text(encoding="utf-8")
    except OSError:
        return None


def write_note(rel: str, text: str) -> bool:
    """Create/overwrite a note atomically. Returns success."""
    p = _resolve(rel)
    if p is None:
        return False
    try:
        _atomic_write(p, text)
        return True
    except OSError:
        return False


def append_note(rel: str, text: str, heading: Optional[str] = None) -> bool:
    """Append to a note (creates it if missing). Optional dated heading."""
    p = _resolve(rel)
    if p is None:
        return False
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    block = ""
    if heading:
        block += f"\n\n## {heading} · {stamp}\n"
    block += ("\n" if not heading else "") + text.rstrip() + "\n"
    try:
        existing = p.read_text(encoding="utf-8") if p.is_file() else ""
        _atomic_write(p, existing.rstrip("\n") + block if existing else block.lstrip("\n"))
        return True
    except OSError:
        return False


def latest_notes(subdir: str, n: int = 5) -> list[dict]:
    """Last N notes in a subdirectory, newest first.
    Returns [{rel, name, mtime, preview}] — feeds the HUD Vault/Feedback panels."""
    d = _resolve(subdir)
    if d is None or not d.is_dir():
        return []
    files = sorted(
        (f for f in d.rglob("*.md")),
        key=lambda f: f.stat().st_mtime,
        reverse=True,
    )[:n]
    out = []
    for f in files:
        try:
            text = f.read_text(encoding="utf-8")
        except OSError:
            text = ""
        preview = " ".join(text.split())[:160]
        out.append({
            "rel": str(f.relative_to(VAULT_ROOT)).replace("\\", "/"),
            "name": f.name,
            "mtime": datetime.fromtimestamp(f.stat().st_mtime).isoformat(timespec="minutes"),
            "preview": preview,
        })
    return out


def daily_note(text: str) -> bool:
    """Append a learning to today's daily note (daily/YYYY-MM-DD.md)."""
    return append_note(f"daily/{date.today().isoformat()}.md", f"- {text.strip()}")


# --------------------------------------------------------------------------
# Patterns — the Phase 2 hot path (mtime-cached)
# --------------------------------------------------------------------------

_patterns_cache: dict = {"mtime": None, "text": None, "active": None}

_PATTERNS_REL = "brain/Patterns.md"


def read_patterns() -> Optional[str]:
    """Full Patterns.md text, cached by mtime."""
    p = _resolve(_PATTERNS_REL)
    if p is None or not p.is_file():
        return None
    try:
        m = p.stat().st_mtime
    except OSError:
        return None
    if _patterns_cache["mtime"] != m:
        try:
            _patterns_cache["text"] = p.read_text(encoding="utf-8")
            _patterns_cache["mtime"] = m
            _patterns_cache["active"] = _parse_active(_patterns_cache["text"])
        except OSError:
            return None
    return _patterns_cache["text"]


def _parse_active(text: str) -> list[dict]:
    """Parse '## Active' section into [{category, rule}].
    Expected format inside ## Active:
        ### dental
        - rule text
        - rule text
        ### all
        - rule text
    A '### all' block applies to every category."""
    m = re.search(r"^##\s+Active\s*$(.*?)(?=^##\s|\Z)", text, re.M | re.S)
    if not m:
        return []
    body = m.group(1)
    out, cat = [], "all"
    for line in body.splitlines():
        h = re.match(r"^###\s+(.+?)\s*$", line)
        if h:
            cat = h.group(1).strip().lower()
            continue
        b = re.match(r"^\s*[-*]\s+(.+?)\s*$", line)
        if b:
            out.append({"category": cat, "rule": b.group(1)})
    return out


def get_active_patterns(category: Optional[str] = None) -> list[str]:
    """Active pattern rules for a category (+ 'all' rules). Hot path for
    enrichment.py — call freely, it's one os.stat when unchanged."""
    read_patterns()  # refresh cache if stale
    rules = _patterns_cache["active"] or []
    cat = (category or "all").strip().lower()
    return [r["rule"] for r in rules if r["category"] in ("all", cat)]


def patterns_block(category: Optional[str] = None) -> str:
    """Ready-to-inject LEARNED PATTERNS block for build_outreach_prompt().
    Empty string when no patterns — safe to always concatenate."""
    rules = get_active_patterns(category)
    if not rules:
        return ""
    lines = "\n".join(f"- {r}" for r in rules)
    return (
        "\nLEARNED PATTERNS (from real campaign outcomes — apply these):\n"
        f"{lines}\n"
    )


# --------------------------------------------------------------------------
# Manifest — feeds the HUD Vault bubble view
# --------------------------------------------------------------------------

def manifest() -> dict:
    """{folders:[{name, files:[{name, kb}]}]} for GET /api/vault/manifest."""
    folders = []
    root_files = []
    if VAULT_ROOT.is_dir():
        for f in VAULT_ROOT.glob("*.md"):
            root_files.append({"name": f.name, "kb": max(1, f.stat().st_size // 1024)})
    if root_files:
        folders.append({"name": "root", "files": root_files})
    for name in MANIFEST_FOLDERS:
        d = VAULT_ROOT / name
        if not d.is_dir():
            continue
        files = [
            {
                "name": str(f.relative_to(d)).replace("\\", "/"),
                "kb": max(1, f.stat().st_size // 1024),
            }
            for f in sorted(d.rglob("*.md"))
        ]
        if files:
            folders.append({"name": name, "files": files})
    return {"folders": folders, "root": str(VAULT_ROOT)}


# --------------------------------------------------------------------------
# Self-test:  python vault.py
# --------------------------------------------------------------------------
if __name__ == "__main__":
    print("VAULT_ROOT:", VAULT_ROOT, "| exists:", VAULT_ROOT.is_dir())
    ok = write_note("daily/_selftest.md", "# self test\n- vault.py write ok\n")
    print("write:", ok)
    print("read:", (read_note("daily/_selftest.md") or "")[:40], "…")
    print("append:", append_note("daily/_selftest.md", "- appended line"))
    print("latest daily:", [n["name"] for n in latest_notes("daily", 3)])
    print("active patterns (dental):", get_active_patterns("dental"))
    print("patterns_block:", repr(patterns_block("dental")[:80]))
    print("manifest folders:", [f["name"] for f in manifest()["folders"]])
    print("traversal guard:", read_note("../../.env") is None and "OK" or "FAIL")
