import test from 'node:test';
import assert from 'node:assert/strict';
import { migrationQueuePolicy } from '../src/migrationQueueControl.js';

const base = { scheduler: { status: 'finishing' }, batch: { status: 'running' },
  pressure: { channelReady: 10, detailReady: 10, detailBacklog: 0 } };
test('explicit discovery resume after migration completion retains capacity and backlog limits', () => {
  const value = { ...base, batch: { status: 'completed' },
    scheduler: { status: 'stopped', stop_reason: 'pipeline_complete' }, discoveryEnabled: true,
    pressure: { ...base.pressure, discoverReady: 2, channelBacklog: 0, agentBacklog: 0, dataApiBacklog: 0 } };
  const discover = input => migrationQueuePolicy(input)['youtube-discover-page'];
  assert.deepEqual(discover(value), { paused: false, reason: 'operator_resumed' });
  assert.equal(discover({ ...value, discoveryEnabled: false }).paused, true);
  assert.equal(discover({ ...value, batch: { status: 'running' } }).paused, true);
  assert.equal(discover({ ...value, scheduler: { status: 'paused' } }).paused, true);
  assert.equal(discover({ ...value, pressure: { ...value.pressure, discoverReady: null } }).reason, 'proxy_capacity_unavailable');
  assert.equal(discover({ ...value, pressure: { ...value.pressure, discoverReady: 0 } }).reason, 'proxy_capacity_low');
  assert.equal(discover({ ...value, pressure: { ...value.pressure, channelBacklog: 101 } }).reason, 'channel_backlog_high');
  assert.equal(discover({ ...value, pressure: { ...value.pressure, agentBacklog: 11 } }).reason, 'agent_backlog_high');
  const middle = { ...value, pressure: { ...value.pressure, channelBacklog: 60 } };
  assert.equal(discover({ ...middle, discoveryPaused: true }).paused, true);
  assert.equal(discover({ ...middle, discoveryPaused: false }).paused, false);
});
test('unknown network capacity holds crawl without blocking Agent and Finalize', () => {
  const p = migrationQueuePolicy({ ...base, pressure: { channelReady: null } });
  assert.equal(p['youtube-channel-crawl'].reason, 'proxy_capacity_unavailable');
  assert.equal(p['youtube-agent-batch'].paused, false);
  assert.equal(p['youtube-finalize'].paused, false);
});
test('detail pressure respects hysteresis only for separate detail execution', () => {
  const value = { ...base, inlineDetails: false, channelPaused: true,
    pressure: { ...base.pressure, detailBacklog: 60 } };
  assert.equal(migrationQueuePolicy(value)['youtube-channel-crawl'].paused, true);
  assert.equal(migrationQueuePolicy({ ...value, channelPaused: false })['youtube-channel-crawl'].paused, false);
  assert.equal(migrationQueuePolicy({ ...value, inlineDetails: true })['youtube-channel-crawl'].paused, false);
});
test('automatic completion leaves consumers open including historical detail recovery', () => {
  const p = migrationQueuePolicy({ ...base, batch: { status: 'completed' },
    scheduler: { status: 'stopped', stop_reason: 'pipeline_complete' }, recoveryQueues: ['youtube-content-detail'] });
  assert.equal(p['youtube-content-detail'].paused, false);
  assert.equal(p['youtube-channel-crawl'].paused, false);
  assert.equal(p['youtube-agent-batch'].paused, false);
});
test('user-ended batch never inherits automatic completion recovery', () => {
  const p = migrationQueuePolicy({ ...base, batch: { status: 'ended' },
    scheduler: { status: 'stopped', stop_reason: 'user_ended_batch' }, recoveryQueues: ['youtube-channel-crawl', 'youtube-agent-batch'] });
  assert.equal(p['youtube-channel-crawl'].paused, true);
  assert.equal(p['youtube-agent-batch'].paused, true);
});

test('completed batch keeps idle crawl, Agent, enabled API and Finalize open without recovery demand', () => {
  const p = migrationQueuePolicy({ ...base, batch: { status: 'completed' },
    scheduler: { status: 'stopped', stop_reason: 'pipeline_complete' } });
  for (const name of ['youtube-channel-crawl', 'youtube-agent-batch', 'youtube-data-api-batch', 'youtube-finalize']) {
    assert.equal(p[name].paused, false, name);
  }
  assert.equal(p['youtube-discover-page'].paused, true);
  assert.equal(p['youtube-content-detail'].paused, true);
});
test('completed batch still respects manual scheduler pause and current network pressure', () => {
  const value = { ...base, batch: { status: 'completed' },
    scheduler: { status: 'stopped', stop_reason: 'pipeline_complete' } };
  assert.equal(migrationQueuePolicy({ ...value, scheduler: { status: 'paused' } })['youtube-channel-crawl'].paused, true);
  assert.equal(migrationQueuePolicy({ ...value, scheduler: { status: 'stopped', stop_reason: 'user_stopped' } })['youtube-agent-batch'].paused, true);
  assert.equal(migrationQueuePolicy({ ...value, pressure: { ...base.pressure, channelReady: 0 } })['youtube-channel-crawl'].reason, 'proxy_capacity_low');
  assert.equal(migrationQueuePolicy({ ...value, apiEnabled: false })['youtube-data-api-batch'].paused, true);
});
