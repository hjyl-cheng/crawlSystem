# Backend / Database Pagination Research

Verified on 2026-08-26 against PostgreSQL 18 official documentation. This
document changes no business code.

Implementation status: the pre-change path described in section B was
replaced on 2026-08-26 by the Target-side inventory recommended in section C.
The observations remain as the measured baseline that motivated the change.

## Conclusion

For an endpoint that promises pagination over a filtered result set, the
required semantics are:

```text
page = slice(stable_sort(filter(full_relation)))
```

Filtering a previously sliced batch is a different operation and cannot in
general produce the requested page. PostgreSQL's documented `SELECT`
processing order implements the required semantics as `WHERE`, then `ORDER
BY`, then `LIMIT/OFFSET`.

`LIMIT/OFFSET` is valid for shallow navigation when the ordering is unique,
but PostgreSQL must still compute skipped rows, so deep offsets get
progressively wasteful. Keyset (cursor/seek) pagination avoids skipping all
preceding rows by continuing after the last ordered key, and benefits from a
B-tree index whose column order and directions match the query.

The Migration Channels page has an architectural complication: its complete
filterable row is assembled from a read-only Source database and the Target
database. The Source-only and Target-only paths already filter before
pagination in SQL. The mixed path cannot apply all filters in either database;
it walks Source batches from offset zero and filters after merging in Node.
The durable fix is a single queryable Target-side read model containing every
eligible Source candidate, including unstarted candidates, overlaid with
Target lifecycle state.

## A. Sourced facts

Everything in this section is based on PostgreSQL's official documentation.
Repo-specific conclusions begin in section B.

### A.1 `SELECT` processing order

PostgreSQL documents the general processing of `SELECT` in ordered steps. The
relevant subsequence is:

1. `FROM` computes the input relation.
2. `WHERE` eliminates rows that do not satisfy its condition.
3. After intervening projection/group/set-operation steps, `ORDER BY` sorts
   the returned rows.
4. `LIMIT`/`FETCH FIRST` and `OFFSET` return only a subset of those result
   rows.

Source: [PostgreSQL 18 `SELECT` documentation](https://www.postgresql.org/docs/18/sql-select.html#SQL-SELECT).

This is the documented logical/general processing model, not a claim about
the executor's physical operator schedule. The optimizer may choose a
different physical plan as long as it preserves the query's semantics.

Therefore, for a filtered endpoint, the normal query shape is:

```sql
SELECT ...
FROM migration_channel_read_model
WHERE <all active filters>
ORDER BY priority DESC, source_candidate_id ASC
LIMIT $1 OFFSET $2;
```

The direct relational consequence of PostgreSQL's processing rules is:

```text
correct: filter -> stable order -> offset/limit
wrong:   stable order -> offset/limit -> filter
```

The two expressions do not commute. For example, if the first ten unfiltered
rows do not match a status filter but rows 11-20 do, slicing ten rows before
filtering returns an empty page; filtering first returns rows 11-20 as the
first filtered page. Paginating first is only correct when the API explicitly
defines its contract as "filter this already selected batch", not "paginate
the filtered result".

### A.2 `LIMIT/OFFSET` behavior

PostgreSQL defines `LIMIT` as the maximum number of rows returned and `OFFSET`
as the number of rows skipped before returning rows. When both are present,
the offset rows are skipped before PostgreSQL starts counting the limited
rows. It also states two important constraints:

- `ORDER BY` should constrain rows into a **unique order**; otherwise different
  `LIMIT/OFFSET` values can yield unpredictable or inconsistent subsets.
- Rows skipped by `OFFSET` still have to be computed inside the server, so a
  large offset can be inefficient.

Source: [PostgreSQL 18, "LIMIT and OFFSET"](https://www.postgresql.org/docs/18/queries-limit.html).

An ordering column with duplicates is therefore insufficient by itself. A
stable unique tiebreaker such as a primary key must complete the ordering, for
example `ORDER BY priority DESC, candidate_id ASC`.

### A.3 Keyset/cursor pagination primitives

PostgreSQL does not prescribe an HTTP cursor format, but it provides the SQL
primitives used for keyset pagination. Row-constructor comparisons support
`<`, `<=`, `>`, and `>=`; PostgreSQL compares row elements left to right and
stops at the first unequal or null pair. The selected comparison operators
must belong to B-tree operator classes (or have the documented equivalent
semantics).

Source: [PostgreSQL 18, "Row Constructor Comparison"](https://www.postgresql.org/docs/18/functions-comparisons.html#ROW-WISE-COMPARISON).

For two descending, non-null sort keys, the next page can use a strict ordered
comparison against the previous page's final row:

```sql
SELECT ...
FROM items
WHERE <unchanged filters>
  AND (created_at, id) < ($cursor_created_at, $cursor_id)
ORDER BY created_at DESC, id DESC
LIMIT $page_size;
```

The strict comparison excludes the boundary row. The cursor must carry every
ordered key needed to locate that boundary.

Mixed directions need matching comparison semantics. For this repo's current
`priority DESC, candidate_id ASC` order, "after this cursor" is:

```sql
WHERE <unchanged filters>
  AND (
    priority < $cursor_priority
    OR (priority = $cursor_priority AND candidate_id > $cursor_candidate_id)
  )
ORDER BY priority DESC, candidate_id ASC
LIMIT $page_size;
```

This expanded predicate is the mixed-direction equivalent of a lexicographic
seek. It also makes the boundary behavior explicit.

### A.4 Index support

Only B-tree indexes can directly produce sorted output in PostgreSQL. A B-tree
whose ordering matches `ORDER BY` can avoid a separate sort. PostgreSQL calls
out `ORDER BY ... LIMIT n` as an important case: with a matching index it can
retrieve the first `n` rows directly instead of processing all data to find
them. For multicolumn mixed ordering, an index can explicitly specify
directions such as `(x DESC, y ASC)`.

Source: [PostgreSQL 18, "Indexes and ORDER BY"](https://www.postgresql.org/docs/18/indexes-ordering.html).

For multicolumn B-tree indexes, PostgreSQL states that equality constraints on
leading columns, plus an inequality constraint on the first following column,
are the most effective at limiting the scanned portion of the index.

Source: [PostgreSQL 18, "Multicolumn Indexes"](https://www.postgresql.org/docs/18/indexes-multicolumn.html).

For a projection scoped by `source_id` and ordered like this page, the natural
starting index to evaluate is therefore:

```sql
CREATE INDEX ... ON migration_channel_read_model
  (source_id, priority DESC, source_candidate_id ASC);
```

That is an index candidate, not a guarantee that the planner will select it
for every optional-filter combination. Production index decisions should be
validated with the real predicates and data distribution.

## B. Pre-change repo observations

These observations were captured before the Target-side inventory was
implemented; they are not claims made by the PostgreSQL documentation.

1. `loadMigrationSourcePage` builds `ranked_source`, filters to the winning row
   per channel, pending Source statuses, and search text, then applies
   `ORDER BY priority DESC, candidate_id`, `LIMIT`, and `OFFSET` inside
   `source_page`. Only then does it join back to load wide JSON columns. That
   path already has the right Source-query order. See
   `services/dashboard/src/migrationTopology.js:90-141`.
2. `migrationTargetPage` applies search and Target lifecycle filters before
   `ORDER BY`, `LIMIT`, and `OFFSET`. That Target-only path also follows the
   required order. See `services/dashboard/src/server.js:1676-1707`.
3. `migrationSourceFilteredPage` is different. It starts at Source offset zero,
   loads batches of 500, queries Target rows for each batch, merges in Node,
   applies final filters in Node, counts matches until the requested offset,
   and continues until the page is full. See
   `services/dashboard/src/server.js:1713-1735`.
4. The mixed path is not merely a misplaced SQL clause: Source and Target
   facts are in different PostgreSQL databases, so neither existing query can
   see the full logical row on which all filters operate.
5. The list currently defaults to 500 rows per page and permits 1-500. See
   `services/dashboard/src/server.js:1737-1746`.
6. `candidate_id` is the Source table primary key and `priority` is non-null,
   so they form a suitable deterministic boundary pair. See
   `services/qybullmq/src/schema.sql:214-227`.

The current mixed loop can fill a logically filtered page by scanning enough
chunks, but its work and cross-database round trips grow with the number of
Source candidates preceding the requested matches. It is application-side
scanning over database pages, not one database pagination query over the final
filtered relation.

## C. Repo-specific recommendation

### C.1 Correct data boundary first

Create a Target-side Migration Channels read model/projection with one narrow
row per eligible Source candidate, including candidates whose migration has
not started. Overlay Target status fields there. The list endpoint can then
apply search/status filters, stable ordering, and pagination to one relation
in one SQL statement. Keep detail JSON and content aggregates out of the
initial page CTE and load them only for selected IDs, matching the good pattern
already used by `loadMigrationSourcePage`.

This projection is preferable to trying to push Target predicates into the
read-only Source query: those columns do not exist in Source. A cross-database
join mechanism could also create one queryable relation, but it adds runtime
coupling to the deliberately isolated migration Source.

### C.2 Immediate page contract

Use a conservative default page size of 50 (with a validated cap), and always
order by `priority DESC, source_candidate_id ASC`. Apply every active filter in
the same filtered base query before pagination. If an exact filtered total is
required, calculate it from the same predicate; navigation alone can request
`page_size + 1` rows and use the extra row only to determine `has_next`.

Offset pagination is acceptable as an interim UI contract for shallow
previous/next navigation once it operates on the consolidated relation. It
should not be presented as a scalable deep-pagination solution.

### C.3 Move deep navigation to a cursor

Return an opaque cursor encoding at least `priority` and
`source_candidate_id`. Bind or validate it against the active filters so a
cursor from one result set cannot silently continue another. The next-page
query should use the mixed-direction seek predicate shown in A.3 and the
matching `(source_id, priority DESC, source_candidate_id ASC)` B-tree index.

Cursor pagination trades arbitrary page-number jumps for efficient sequential
navigation. If arbitrary jumps are a hard product requirement, retain offset
pagination for that operation and accept/measure its deep-offset cost.

### C.4 Verification criteria for an implementation

- A sparse filter whose first match occurs after Source row 500 still returns
  a full first page without scanning from zero in Node.
- Equal-priority rows crossing a page boundary have no duplicates or omissions.
- Concatenating consecutive pages equals the same filtered relation queried
  without pagination under a fixed test snapshot.
- Changing any search/status filter invalidates or rejects the old cursor.
- Query-plan checks on production-like cardinality confirm that the selected
  IDs are bounded before wide JSON and aggregate loading.

## Official sources

All URLs below returned HTTP 200 when verified on 2026-08-26. No secondary
sources were used.

1. [PostgreSQL 18: `SELECT`](https://www.postgresql.org/docs/18/sql-select.html#SQL-SELECT)
2. [PostgreSQL 18: `LIMIT` and `OFFSET`](https://www.postgresql.org/docs/18/queries-limit.html)
3. [PostgreSQL 18: Row Constructor Comparison](https://www.postgresql.org/docs/18/functions-comparisons.html#ROW-WISE-COMPARISON)
4. [PostgreSQL 18: Indexes and `ORDER BY`](https://www.postgresql.org/docs/18/indexes-ordering.html)
5. [PostgreSQL 18: Multicolumn Indexes](https://www.postgresql.org/docs/18/indexes-multicolumn.html)
