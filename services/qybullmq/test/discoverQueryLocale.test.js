import assert from 'node:assert/strict';
import test from 'node:test';

import { resolveDiscoverQueryLocale } from '../src/discoverQueryLocale.js';
import { loadIdentityPolicyCatalog } from '../src/identityPolicyCatalog.js';
import { buildDiscoverPageIntent, ManagedPolicyUnavailableError } from '../src/managedJobIntents.js';

const policies = [...loadIdentityPolicyCatalog().policies.values()];
const intent = locale => buildDiscoverPageIntent({ pageId: 'query:1:run:x:page:1', queryText: 'receitas', queryId: 1, pageNo: 1,
  discoveryRunId: 'query:1:run:x', pipelineCycleId: 'pipeline:x', dispatchBatchId: 'pipeline:x', ...locale }, { policies });

test('an imported base-language Query without a country uses the single Discover policy of that language', () => {
  const locale = resolveDiscoverQueryLocale({ language: 'pt', country: null }, policies);
  assert.deepEqual(locale, { language: 'pt-BR', country: 'BR' });
  assert.equal(intent(locale).policy.id, 'qy-br-discover-anonymous-v1');
  assert.deepEqual(resolveDiscoverQueryLocale({ language: 'PT', country: 'br' }, policies), { language: 'pt-BR', country: 'BR' });
  assert.deepEqual(resolveDiscoverQueryLocale({ language: 'pt-BR', country: 'BR' }, policies), { language: 'pt-BR', country: 'BR' });
});

test('a regional language, an explicit other country or an unknown language is never rewritten', () => {
  for (const [input, expected] of [
    [{ language: 'pt-PT', country: null }, { language: 'pt-PT', country: null }],
    [{ language: 'pt', country: 'PT' }, { language: 'pt', country: 'PT' }],
    [{ language: 'en', country: '' }, { language: 'en', country: null }],
    [{ language: '', country: 'BR' }, { language: null, country: 'BR' }],
  ]) assert.deepEqual(resolveDiscoverQueryLocale(input, policies), expected);
  assert.throws(() => intent({ language: 'pt', country: 'PT' }), ManagedPolicyUnavailableError);
  assert.throws(() => intent({ language: 'pt-PT', country: null }), /country is required/);
});

test('two Discover policies for one language are ambiguous and keep the Query locale', () => {
  const discover = policies.find(policy => policy.role === 'discover');
  const pt = { ...discover, id: 'qy-pt-discover', youtube_language: 'pt-PT', youtube_country: 'PT' };
  assert.deepEqual(resolveDiscoverQueryLocale({ language: 'pt', country: null }, [...policies, pt]), { language: 'pt', country: null });
  assert.deepEqual(resolveDiscoverQueryLocale({ language: 'pt', country: 'PT' }, [...policies, pt]), { language: 'pt-PT', country: 'PT' });
});
