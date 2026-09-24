import assert from 'node:assert/strict';
import test from 'node:test';
import { buildBusinessPublicationProjection } from '../src/businessPublicationProjectionAdapter.js';
import { completeInput } from './support/businessProjectionFixture.js';

test('latest publication reuses video and profile identities across About updates', () => {
  const input = completeInput();
  input.storageMode = 'latest';
  const first = buildBusinessPublicationProjection(input);
  input.batchId = 'next-about-publication';
  input.versionVector.channel.sequence++;
  input.current.channel.payload_json.subscriber_count++;
  const next = buildBusinessPublicationProjection(input);
  assert.equal(next.snapshot.id, first.snapshot.id);
  assert.deepEqual(next.contents, first.contents);
  assert.deepEqual(next.facts, first.facts);
  assert.notEqual(next.snapshot.subscriber_count, first.snapshot.subscriber_count);
});

test('legacy publication retains immutable snapshot identities', () => {
  const input = completeInput();
  const first = buildBusinessPublicationProjection(input);
  input.batchId = 'next-about-publication';
  assert.notEqual(buildBusinessPublicationProjection(input).snapshot.id, first.snapshot.id);
});
