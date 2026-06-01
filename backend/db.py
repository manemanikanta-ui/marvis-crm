from __future__ import annotations

import os
import re
import sqlite3
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Iterable, Iterator, List, Optional, Sequence, Tuple

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
SQLITE_DB_PATH = BASE_DIR / "data" / "crm.db"
USE_POSTGRES = bool(os.getenv("DATABASE_URL") or os.getenv("DATABASE_PUBLIC_URL"))

if USE_POSTGRES:
    import psycopg2
    import psycopg2.extras


class RowProxy(Mapping):
    def __init__(self, columns: Sequence[str], values: Sequence[Any]):
        self._columns = list(columns)
        self._values = list(values)
        self._data = dict(zip(self._columns, self._values))

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, int):
            return self._values[key]
        return self._data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._columns)

    def __len__(self) -> int:
        return len(self._columns)

    def __repr__(self) -> str:
        return f"RowProxy({self._data!r})"

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)


class ResultProxy:
    def __init__(self, cursor: Any, *, lastrowid: Optional[int] = None):
        self._cursor = cursor
        self.lastrowid = lastrowid

    @property
    def rowcount(self) -> int:
        return getattr(self._cursor, "rowcount", -1)

    @property
    def description(self):
        return getattr(self._cursor, "description", None)

    def _wrap_row(self, row: Any):
        if row is None:
            return None
        if not USE_POSTGRES:
            return row
        columns = [desc[0] for desc in (self._cursor.description or [])]
        return RowProxy(columns, row)

    def fetchone(self):
        return self._wrap_row(self._cursor.fetchone())

    def fetchall(self):
        rows = self._cursor.fetchall()
        if not USE_POSTGRES:
            return rows
        columns = [desc[0] for desc in (self._cursor.description or [])]
        return [RowProxy(columns, row) for row in rows]

    def __iter__(self):
        while True:
            row = self.fetchone()
            if row is None:
                break
            yield row


def placeholder() -> str:
    return "%s" if USE_POSTGRES else "?"


def now_sql() -> str:
    return "NOW()" if USE_POSTGRES else "datetime('now')"


def _convert_placeholders(sql: str) -> str:
    if not USE_POSTGRES:
        return sql
    out = []
    in_single = False
    param_count = 0
    for ch in sql:
        if ch == "'":
            in_single = not in_single
            out.append(ch)
        elif ch == "?" and not in_single:
            param_count += 1
            out.append("%s")
        else:
            out.append(ch)
    return "".join(out)


def _translate_settings_upsert(sql: str) -> str:
    if not USE_POSTGRES:
        return sql

    lowered = sql.lower()
    if "insert or ignore into settings" in lowered:
        return re.sub(
            r"^\s*INSERT\s+OR\s+IGNORE\s+INTO\s+settings(?:\s*\(key,\s*value\))?\s+VALUES\s*\((.*?)\)\s*$",
            r"INSERT INTO settings (key, value) VALUES (\1) ON CONFLICT (key) DO NOTHING",
            sql,
            flags=re.IGNORECASE | re.DOTALL,
        )

    if "insert or replace into settings" in lowered:
        return re.sub(
            r"^\s*INSERT\s+OR\s+REPLACE\s+INTO\s+settings(?:\s*\(key,\s*value\))?\s+VALUES\s*\((.*?)\)\s*$",
            r"INSERT INTO settings (key, value) VALUES (\1) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
            sql,
            flags=re.IGNORECASE | re.DOTALL,
        )

    return sql


def _translate_schema(sql: str) -> str:
    if not USE_POSTGRES:
        return sql
    sql = sql.replace("AUTOINCREMENT", "SERIAL PRIMARY KEY")
    sql = sql.replace("datetime('now')", "NOW()")
    sql = sql.replace("(datetime('now'))", "NOW()")
    sql = sql.replace(" DEFAULT (datetime('now'))", " DEFAULT NOW()")
    sql = sql.replace(" DEFAULT datetime('now')", " DEFAULT NOW()")
    return sql


def _parse_insert_target(sql: str) -> Optional[str]:
    match = re.match(r"^\s*INSERT\s+(?:OR\s+\w+\s+)?INTO\s+([A-Za-z0-9_]+)", sql, flags=re.IGNORECASE)
    return match.group(1).lower() if match else None


def _should_returning_id(sql: str) -> bool:
    if not USE_POSTGRES:
        return False
    lowered = sql.lower()
    if "returning" in lowered:
        return False
    table = _parse_insert_target(sql)
    return bool(table and table not in {"settings"})


def _translate_sql(sql: str) -> str:
    sql = _translate_settings_upsert(sql)
    sql = _translate_schema(sql)
    sql = _convert_placeholders(sql)
    return sql


class DBConnection:
    def __init__(self, conn: Any):
        self._conn = conn

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None):
        if USE_POSTGRES:
            translated = _translate_sql(sql)
            final_params = tuple(params or ())
            needs_returning = _should_returning_id(translated)
            if needs_returning:
                translated = translated.rstrip().rstrip(";") + " RETURNING id"
            cursor = self._conn.cursor()
            cursor.execute(translated, final_params)
            lastrowid = None
            if needs_returning:
                row = cursor.fetchone()
                if row:
                    lastrowid = row[0]
            return ResultProxy(cursor, lastrowid=lastrowid)

        cursor = self._conn.execute(sql, tuple(params or ()))
        return ResultProxy(cursor, lastrowid=getattr(cursor, "lastrowid", None))

    def executemany(self, sql: str, seq_of_params: Iterable[Sequence[Any]]):
        if USE_POSTGRES:
            cursor = self._conn.cursor()
            translated = _translate_sql(sql)
            cursor.executemany(translated, [tuple(params) for params in seq_of_params])
            return ResultProxy(cursor)
        cursor = self._conn.executemany(sql, seq_of_params)
        return ResultProxy(cursor)

    def executescript(self, script: str):
        if USE_POSTGRES:
            result = None
            for statement in script.split(";"):
                stmt = statement.strip()
                if not stmt:
                    continue
                result = self.execute(stmt)
            return result
        return self._conn.executescript(script)

    def commit(self):
        return self._conn.commit()

    def close(self):
        return self._conn.close()

    def cursor(self):
        return self._conn.cursor()

    def __getattr__(self, name: str):
        return getattr(self._conn, name)


def get_db() -> DBConnection:
    if USE_POSTGRES:
        url = os.getenv("DATABASE_URL") or os.getenv("DATABASE_PUBLIC_URL")
        conn = psycopg2.connect(url, cursor_factory=psycopg2.extras.Cursor)
        conn.autocommit = False
        return DBConnection(conn)

    SQLITE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(SQLITE_DB_PATH)
    conn.row_factory = sqlite3.Row
    return DBConnection(conn)


def table_columns(conn: Optional[DBConnection], table: str) -> List[str]:
    created = False
    if conn is None:
        conn = get_db()
        created = True

    try:
        if USE_POSTGRES:
            rows = conn.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
                """,
                (table.lower(),),
            ).fetchall()
            return [row[0] for row in rows]

        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return [row[1] for row in rows]
    finally:
        if created:
            conn.close()
