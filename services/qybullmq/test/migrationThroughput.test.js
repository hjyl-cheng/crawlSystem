import assert from 'node:assert/strict';
import test from 'node:test';
import { migrationRollingRates } from '../src/migrationThroughput.js';

test('rolling settlement rates use the same batch, exclude pauses and reject corrected counters', () => {
  const batch = { batch_id: 'current', active_seconds: 3600, counts: { success: 1000, dormant: 200 } };
  const now = new Date('2026-09-10T10:00:00Z');
  const samples = [
    { batch_id: 'other', sampled_at: '2026-09-10T09:45:00Z', active_seconds: 1, counts: {} },
    { batch_id: 'current', sampled_at: '2026-09-10T09:45:00Z', active_seconds: 3000, counts: { success: 800, dormant: 100 } },
    { batch_id: 'current', sampled_at: '2026-09-10T09:00:00Z', active_seconds: 600, counts: { success: 200 } },
  ];
  const rates = migrationRollingRates(batch, samples, now);
  assert.equal(rates.minutes_15.per_hour, 1800);
  assert.equal(rates.minutes_15.completed, 300);
  assert.equal(rates.minutes_60.per_hour, 1200);
  assert.equal(migrationRollingRates({ ...batch, counts: { success: 1 } }, samples, now).minutes_15, null);
  assert.equal(migrationRollingRates(batch, [], now).minutes_15, null);
  const fetched = migrationRollingRates({ ...batch, counts: { ...batch.counts, fetch_completed: 100 } },
    samples.map(row => ({ ...row, counts: { ...row.counts, fetch_completed: 50 } })), now).minutes_15;
  assert.equal(fetched.fetch_completed, 50);
  assert.equal(fetched.fetch_per_hour, 300);
  assert.equal(rates.minutes_15.fetch_completed, null);
});
