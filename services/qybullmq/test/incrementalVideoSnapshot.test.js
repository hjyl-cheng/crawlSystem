import test from 'node:test';
import assert from 'node:assert/strict';
import { recentRowsFromSnapshot } from '../src/incrementalVideoSnapshot.js';

test('snapshot sampling preserves stored publication, repairs undated feed evidence, and retains due older repairs', () => {
  const row = (id, published_at, enrich_pending = false) => ({content_key:`channel:${id}`,source_content_id:id,
    published_at,stored_publication:{published_at},enrich_pending});
  const rows = [row('stored','2026-07-01T00:00:00Z'),row('undated',null),row('due','2026-01-01T00:00:00Z',true),row('unknown',null)];
  const scan = ['stored','undated'].map(id=>({id,published_at:'',published_day:'2026-09-10',
    published_at_status:'exact',published_at_precision:'date_only',published_at_source:'youtubejs_feed'}));
  const result=recentRowsFromSnapshot(rows,scan,{planDay:'2026-09-12',recentWindowDays:30});
  assert.deepEqual(result.map(item=>item.source_content_id),['undated','due']);
  assert.equal(result[0].stored_publication.published_at,null,'scan evidence does not rewrite the original stored fact');
  assert.equal(new Date(result[0].published_at).toISOString(),'2026-09-10T00:00:00.000Z');
  assert.equal(rows[1].published_at,null,'immutable dispatch input must not be changed');
});
