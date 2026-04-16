"""
connection.py - PostgreSQL connection management for analyzer4pg
"""

import psycopg2
import psycopg2.extras
from dataclasses import dataclass
from typing import Optional
import sys


@dataclass
class ConnectionConfig:
    host: str = "localhost"
    port: int = 5432
    dbname: str = "postgres"
    user: str = "postgres"
    password: Optional[str] = None
    sslmode: str = "prefer"
    connect_timeout: int = 10

    def to_dsn(self) -> str:
        parts = [
            f"host={self.host}",
            f"port={self.port}",
            f"dbname={self.dbname}",
            f"user={self.user}",
            f"connect_timeout={self.connect_timeout}",
            f"sslmode={self.sslmode}",
        ]
        if self.password:
            parts.append(f"password={self.password}")
        return " ".join(parts)


class DatabaseConnection:
    """Manages a single PostgreSQL connection with utility methods."""

    def __init__(self, config: ConnectionConfig):
        self.config = config
        self._conn: Optional[psycopg2.extensions.connection] = None
        self.server_version: str = ""
        self.server_version_num: int = 0

    def connect(self) -> None:
        try:
            self._conn = psycopg2.connect(
                self.config.to_dsn(),
                cursor_factory=psycopg2.extras.DictCursor,
            )
            self._conn.set_session(autocommit=True)
            self._fetch_server_info()
        except psycopg2.OperationalError as e:
            raise ConnectionError(f"Cannot connect to PostgreSQL: {e}") from e

    def _fetch_server_info(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute("SELECT version(), current_setting('server_version_num')::int")
            row = cur.fetchone()
            self.server_version = row[0]
            self.server_version_num = row[1]

    @property
    def conn(self) -> psycopg2.extensions.connection:
        if self._conn is None or self._conn.closed:
            raise ConnectionError("Not connected. Call connect() first.")
        return self._conn

    def cursor(self):
        return self.conn.cursor()

    def explain_query(self, query: str, use_analyze: bool = True) -> dict:
        """
        Run EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) on a query.
        DML statements are wrapped in a transaction that is always rolled back.
        Returns the plan dict from PostgreSQL.
        """
        query_stripped = query.strip()
        query_upper = query_stripped.upper().lstrip("(")

        is_dml = any(
            query_upper.startswith(kw)
            for kw in ("INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE")
        )

        if use_analyze:
            options = "ANALYZE, BUFFERS, FORMAT JSON"
        else:
            options = "FORMAT JSON"

        explain_sql = f"EXPLAIN ({options}) {query_stripped}"

        try:
            with self._conn.cursor() as cur:
                if is_dml and use_analyze:
                    # Wrap in transaction and roll back to avoid side effects
                    cur.execute("BEGIN")
                    try:
                        cur.execute(explain_sql)
                        result = cur.fetchone()[0]
                    finally:
                        cur.execute("ROLLBACK")
                else:
                    cur.execute(explain_sql)
                    result = cur.fetchone()[0]

            # PostgreSQL returns a list with one element
            return result[0] if isinstance(result, list) else result

        except psycopg2.Error as e:
            raise RuntimeError(f"EXPLAIN failed: {e}") from e

    def fetch_table_stats(self, schema: str, table: str) -> dict:
        """Fetch statistics for a table from pg_stat_user_tables and pg_class."""
        sql = """
            SELECT
                s.n_live_tup,
                s.n_dead_tup,
                s.last_analyze,
                s.last_autoanalyze,
                c.relpages,
                c.reltuples,
                pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
                pg_total_relation_size(c.oid) AS total_size_bytes
            FROM pg_stat_user_tables s
            JOIN pg_class c ON c.relname = s.relname
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND s.relname = %s
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, (schema, table))
            row = cur.fetchone()
            if row:
                return dict(row)
        return {}

    def fetch_existing_indexes(self, schema: str, table: str) -> list:
        """Fetch all indexes for a table."""
        sql = """
            SELECT
                i.relname AS index_name,
                ix.indisunique AS is_unique,
                ix.indisprimary AS is_primary,
                ix.indisvalid AS is_valid,
                array_to_string(ARRAY(
                    SELECT pg_get_indexdef(ix.indexrelid, k + 1, true)
                    FROM generate_subscripts(ix.indkey, 1) AS k
                    ORDER BY k
                ), ', ') AS columns,
                pg_get_indexdef(ix.indexrelid) AS index_def,
                s.idx_scan,
                s.idx_tup_read,
                s.idx_tup_fetch,
                pg_size_pretty(pg_relation_size(i.oid)) AS index_size
            FROM pg_index ix
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = ix.indexrelid
            WHERE n.nspname = %s AND t.relname = %s
            ORDER BY ix.indisprimary DESC, ix.indisunique DESC, i.relname
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, (schema, table))
            return [dict(row) for row in cur.fetchall()]

    def fetch_unused_indexes(self) -> list:
        """Fetch indexes with zero scans (potential candidates for removal)."""
        sql = """
            SELECT
                schemaname,
                relname AS table_name,
                indexrelname AS index_name,
                idx_scan,
                pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
                pg_get_indexdef(indexrelid) AS index_def
            FROM pg_stat_user_indexes
            WHERE idx_scan = 0
              AND indexrelid NOT IN (
                  SELECT conindid FROM pg_constraint
                  WHERE contype IN ('p', 'u')
              )
            ORDER BY pg_relation_size(indexrelid) DESC
            LIMIT 20
        """
        with self._conn.cursor() as cur:
            cur.execute(sql)
            return [dict(row) for row in cur.fetchall()]

    def get_current_database(self) -> str:
        with self._conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            return cur.fetchone()[0]

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


def build_connection_config(
    host: str,
    port: int,
    dbname: str,
    user: str,
    password: Optional[str],
    sslmode: str = "prefer",
) -> ConnectionConfig:
    return ConnectionConfig(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        sslmode=sslmode,
    )
