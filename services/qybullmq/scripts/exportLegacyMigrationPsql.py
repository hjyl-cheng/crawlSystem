#!/usr/bin/env python3
import argparse
import csv
import hashlib
import json
import sqlite3
import sys
from pathlib import Path
from urllib.parse import quote


REQUIRED_COLUMNS = {
    "channel_id",
    "url",
    "name",
    "handle",
    "subscribers",
    "country",
    "is_target",
    "target_reason",
    "short_circuit",
    "error",
    "description",
    "discovered_at",
    "br_evidence_score",
    "br_evidence_reasons",
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Stream results.db channels as an idempotent PostgreSQL migration script.",
    )
    parser.add_argument("--database", default="results.db", help="SQLite source database")
    parser.add_argument("--batch-id", default="legacy-results-full-v1")
    parser.add_argument("--limit", type=int, default=None, help="Optional row limit for a pilot export")
    return parser.parse_args()


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sql_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def bool_or_none(value):
    return None if value is None else bool(value)


def pg_text(value):
    return value.replace("\x00", "") if isinstance(value, str) else value


def main():
    args = parse_args()
    database_path = Path(args.database).resolve()
    if not database_path.is_file():
        raise SystemExit(f"SQLite database not found: {database_path}")
    if args.limit is not None and args.limit <= 0:
        raise SystemExit("--limit must be greater than zero")

    database_uri = f"file:{quote(database_path.as_posix())}?mode=ro"
    connection = sqlite3.connect(database_uri, uri=True)
    connection.row_factory = sqlite3.Row
    columns = {row[1] for row in connection.execute("PRAGMA table_info(channels)")}
    missing_columns = sorted(REQUIRED_COLUMNS - columns)
    if missing_columns:
        raise SystemExit(f"results.db channels table is missing columns: {', '.join(missing_columns)}")
    if "avatar_url" in columns:
        avatar_expression = "avatar_url"
        avatar_source = "results.db.channels.avatar_url"
    else:
        avatar_expression = "NULL AS avatar_url"
        avatar_source = "not_available_in_results_db"

    total_rows = connection.execute("SELECT count(*) FROM channels").fetchone()[0]
    export_count = min(total_rows, args.limit) if args.limit is not None else total_rows
    distinct_ids = connection.execute(
        "SELECT count(DISTINCT channel_id) FROM (SELECT channel_id FROM channels ORDER BY rowid LIMIT ?)",
        (export_count,),
    ).fetchone()[0]
    if distinct_ids != export_count:
        raise SystemExit(f"source channel IDs are not unique: rows={export_count} distinct={distinct_ids}")

    source_hash = file_sha256(database_path)
    batch_id = args.batch_id
    page_id = f"{batch_id}:page:1"
    metadata = {
        "source": "legacy_results_db",
        "source_database": database_path.name,
        "source_database_sha256": source_hash,
        "source_total_rows": total_rows,
        "exported_rows": export_count,
        "avatar_source": avatar_source,
    }

    output = sys.stdout
    output.write("\\set ON_ERROR_STOP on\n")
    output.write("BEGIN;\n")
    output.write("SET LOCAL statement_timeout = 0;\n")
    output.write("SET LOCAL lock_timeout = '10s';\n")
    output.write("""
CREATE TEMP TABLE migration_seed (
  source_rank INTEGER NOT NULL,
  source_rowid BIGINT NOT NULL,
  channel_id TEXT NOT NULL,
  channel_url TEXT NOT NULL,
  handle TEXT,
  title TEXT,
  description TEXT,
  avatar_url TEXT,
  search_subscriber_count BIGINT,
  source_json JSONB NOT NULL
) ON COMMIT DROP;
COPY migration_seed (
  source_rank,source_rowid,channel_id,channel_url,handle,title,description,
  avatar_url,search_subscriber_count,source_json
) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '');
""")

    writer = csv.writer(output, delimiter="\t", lineterminator="\n")
    query = f"""
        SELECT rowid AS source_rowid,channel_id,url,name,handle,subscribers,country,
               is_target,target_reason,short_circuit,error,description,discovered_at,
               br_evidence_score,br_evidence_reasons,{avatar_expression}
        FROM channels
        ORDER BY rowid
        LIMIT ?
    """
    for source_rank, row in enumerate(connection.execute(query, (export_count,)), start=1):
        sanitized_columns = [
            column
            for column in (
                "channel_id",
                "url",
                "name",
                "handle",
                "country",
                "target_reason",
                "error",
                "description",
                "br_evidence_reasons",
                "avatar_url",
            )
            if isinstance(row[column], str) and "\x00" in row[column]
        ]
        source_json = {
            "source": "legacy_results_db",
            "source_database": database_path.name,
            "source_database_sha256": source_hash,
            "source_rowid": row["source_rowid"],
            "avatar_source": avatar_source,
            "legacy_import": {
                "country": pg_text(row["country"]),
                "is_target": bool_or_none(row["is_target"]),
                "target_reason": pg_text(row["target_reason"]),
                "short_circuit": bool_or_none(row["short_circuit"]),
                "error": pg_text(row["error"]),
                "discovered_at": row["discovered_at"],
                "br_evidence_score": row["br_evidence_score"],
                "br_evidence_reasons": pg_text(row["br_evidence_reasons"]),
            },
            **({"source_sanitization": {"removed_nul_from": sanitized_columns}} if sanitized_columns else {}),
        }
        writer.writerow([
            source_rank,
            row["source_rowid"],
            pg_text(row["channel_id"]),
            pg_text(row["url"]),
            pg_text(row["handle"]),
            pg_text(row["name"]),
            pg_text(row["description"]),
            pg_text(row["avatar_url"]),
            row["subscribers"],
            json.dumps(source_json, ensure_ascii=True, separators=(",", ":")),
        ])
    connection.close()
    output.write("\\.\n")

    batch = sql_literal(batch_id)
    page = sql_literal(page_id)
    metadata_json = sql_literal(json.dumps(metadata, ensure_ascii=True, separators=(",", ":")))
    output.write(f"""
INSERT INTO crawler.query_dispatch_batches (
  dispatch_batch_id,pipeline_cycle_id,status,discovered_candidate_count,
  accepted_channel_count,rejected_channel_count,result_json,finished_at,updated_at
) VALUES (
  {batch},{batch},'stopped',{export_count},0,0,{metadata_json}::jsonb,now(),now()
)
ON CONFLICT (dispatch_batch_id) DO UPDATE
SET status='stopped',
    discovered_candidate_count=EXCLUDED.discovered_candidate_count,
    accepted_channel_count=0,
    rejected_channel_count=0,
    result_json=crawler.query_dispatch_batches.result_json || EXCLUDED.result_json,
    finished_at=now(),updated_at=now();

INSERT INTO crawler.query_pages (
  page_id,query_text,page_no,status,accepted_count,candidate_count,
  should_continue,stop_reason,result_json,dispatch_batch_id,finished_at,updated_at
) VALUES (
  {page},'results.db migration',1,'done',0,{export_count},
  false,'migration_seed_loaded',{metadata_json}::jsonb,{batch},now(),now()
)
ON CONFLICT (page_id) DO UPDATE
SET status='done',accepted_count=0,candidate_count=EXCLUDED.candidate_count,
    should_continue=false,stop_reason='migration_seed_loaded',
    result_json=crawler.query_pages.result_json || EXCLUDED.result_json,
    dispatch_batch_id=EXCLUDED.dispatch_batch_id,finished_at=now(),updated_at=now();

INSERT INTO crawler.channel_candidates (
  dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,handle,title,
  description,avatar_url,search_subscriber_count,priority,status,source_json,updated_at
)
SELECT
  {batch},{batch},channel_id,channel_url,handle,title,description,avatar_url,
  search_subscriber_count,100,'discovered',source_json,now()
FROM migration_seed
ORDER BY source_rank
ON CONFLICT (dispatch_batch_id,channel_id) DO UPDATE
SET channel_url=EXCLUDED.channel_url,
    handle=COALESCE(EXCLUDED.handle,crawler.channel_candidates.handle),
    title=COALESCE(EXCLUDED.title,crawler.channel_candidates.title),
    description=COALESCE(EXCLUDED.description,crawler.channel_candidates.description),
    avatar_url=COALESCE(EXCLUDED.avatar_url,crawler.channel_candidates.avatar_url),
    search_subscriber_count=COALESCE(EXCLUDED.search_subscriber_count,crawler.channel_candidates.search_subscriber_count),
    source_json=crawler.channel_candidates.source_json || EXCLUDED.source_json,
    updated_at=now();

INSERT INTO crawler.channel_candidate_sources (
  candidate_id,page_id,query_text,rank_position,discovery_strategy,source_json
)
SELECT
  candidate.candidate_id,{page},'results.db migration',seed.source_rank,
  'legacy_results_db',jsonb_build_object(
    'source_rowid',seed.source_rowid,
    'source_database',seed.source_json->>'source_database',
    'source_database_sha256',seed.source_json->>'source_database_sha256'
  )
FROM migration_seed seed
JOIN crawler.channel_candidates candidate
  ON candidate.dispatch_batch_id={batch} AND candidate.channel_id=seed.channel_id
ORDER BY seed.source_rank
ON CONFLICT (candidate_id,page_id,discovery_strategy) DO UPDATE
SET rank_position=EXCLUDED.rank_position,source_json=EXCLUDED.source_json;

DO $migration_check$
DECLARE
  imported_count INTEGER;
  source_count INTEGER;
  mismatch_count INTEGER;
BEGIN
  SELECT count(*) INTO imported_count
  FROM crawler.channel_candidates
  WHERE dispatch_batch_id={batch};
  IF imported_count <> {export_count} THEN
    RAISE EXCEPTION 'migration candidate count % does not match expected {export_count}', imported_count;
  END IF;

  SELECT count(*) INTO source_count
  FROM crawler.channel_candidate_sources source
  JOIN crawler.channel_candidates candidate ON candidate.candidate_id=source.candidate_id
  WHERE candidate.dispatch_batch_id={batch}
    AND source.page_id={page}
    AND source.discovery_strategy='legacy_results_db';
  IF source_count <> {export_count} THEN
    RAISE EXCEPTION 'migration source count % does not match expected {export_count}', source_count;
  END IF;

  SELECT count(*) INTO mismatch_count
  FROM migration_seed seed
  JOIN crawler.channel_candidates candidate
    ON candidate.dispatch_batch_id={batch} AND candidate.channel_id=seed.channel_id
  WHERE candidate.channel_url IS DISTINCT FROM seed.channel_url
     OR candidate.handle IS DISTINCT FROM seed.handle
     OR candidate.title IS DISTINCT FROM seed.title
     OR candidate.description IS DISTINCT FROM seed.description
     OR candidate.avatar_url IS DISTINCT FROM seed.avatar_url
     OR candidate.search_subscriber_count IS DISTINCT FROM seed.search_subscriber_count
     OR candidate.source_json->>'source_rowid' IS DISTINCT FROM seed.source_rowid::text;
  IF mismatch_count <> 0 THEN
    RAISE EXCEPTION 'migration field mismatch count is %', mismatch_count;
  END IF;
END
$migration_check$;

COMMIT;
SELECT
  {batch} AS dispatch_batch_id,
  count(*)::int AS imported_candidates,
  count(*) FILTER (WHERE avatar_url IS NOT NULL)::int AS candidates_with_avatar
FROM crawler.channel_candidates
WHERE dispatch_batch_id={batch}
GROUP BY dispatch_batch_id;
""")


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        try:
            sys.stdout.close()
        except BrokenPipeError:
            pass
