import assert from 'node:assert/strict';
import test from 'node:test';
import {normalizePublicationLinks} from '../src/publicationLinks.js';
import {normalizePublicationUrl} from '../src/publicationUrl.js';

test('YouTube redirect email targets do not invalidate the entire channel links observation', () => {
  const email = 'https://www.youtube.com/redirect?event=channel_description&q=contact%40example.com';
  const result = normalizePublicationLinks([
    {title:'Business contact',target_url:email},
    {title:'Website',target_url:'https://www.youtube.com/redirect?q=https%3A%2F%2Fexample.com'},
  ], {observed:true});
  assert.equal(result.ready,true);
  assert.equal(result.links.length,2);
  assert.ok(result.links.some(link=>link.target_url==='mailto:contact@example.com'));
  assert.equal(normalizePublicationUrl(email),null);
  assert.equal(normalizePublicationUrl('https://youtube.com/redirect?q=javascript%3Aalert(1)',{allowMailto:true}),null);
});
