import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { normalizeAboutMetrics } from "../src/aboutMetrics.js";
import { recordAboutObservation } from "../src/aboutObservationStore.js";

const { Pool } = pg;
const integrationUrl = process.env.ABOUT_POSTGRES_TEST_URL;

test("About writer accepts initial empties but does not erase a trusted Current with later empties", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 2 });
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UCabout${suffix}`;
  const observedAt = "2026-07-26T12:00:00.000Z";
  const about = normalizeAboutMetrics({
    aboutObserved: true,
    metadata: {
      subscriber_count_text: "123 subscribers",
      subscriber_count_source: "youtube_about",
      view_count_text: "4,567 views",
      view_count_source: "youtube_about",
      video_count_text: "89 videos",
      video_count_source: "youtube_about",
    },
  });

  try {
    await pool.query(
      `INSERT INTO crawler.channels (
         channel_id,channel_url,title,status,summary,about_description,description_status,
         keywords,keywords_status,available_tabs,available_tabs_status,
         external_links,external_links_status,is_verified,is_verified_status,
         youtube_business_email_available,youtube_business_email_observed_at
       ) VALUES (
         $1,$2,'Previous title','active','Previous summary','Previous About','exact',
         ARRAY['previous'],'observed',ARRAY['videos'],'observed',
         '[{"target_url":"https://previous.example"}]'::jsonb,'observed',true,'verified',
         true,'2026-07-25T12:00:00.000Z'
       )`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await recordAboutObservation(client, {
        idempotencyKey: `about:integration:${suffix}`,
        channelId,
        runId: null,
        observedAt,
        triggerReason: "manual",
        crawlerVersion: "integration-test",
        about,
        current: {
          aboutDescription: null,
          descriptionStatus: "empty",
          country: "Brazil",
          joinedDateText: "Joined Jan 2, 2020",
          joinedAt: "2020-01-02",
          joinedAtPrecision: "date_only",
          externalLinks: [],
          externalLinksStatus: "observed",
          isVerified: null,
          isVerifiedStatus: "unknown",
          keywordsStatus: "observed",
          availableTabsStatus: "observed",
          identity: {
            title: "Current title",
            handle: "@current",
            avatar_url: "https://example.test/avatar.jpg",
            keywords: [],
            available_tabs: [],
            summary: null,
          },
        },
      });
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }

    const current = await pool.query(
      `SELECT title,summary,about_description,description_status,
              keywords,keywords_status,available_tabs,available_tabs_status,
              external_links,external_links_status,is_verified,is_verified_status,
              joined_at,joined_at_precision,youtube_business_email_available,
              youtube_business_email_observed_at
       FROM crawler.channels WHERE channel_id=$1`,
      [channelId],
    );
    assert.deepEqual(current.rows[0], {
      title: "Current title",
      summary: "Previous summary",
      about_description: "Previous About",
      description_status: "exact",
      keywords: ["previous"],
      keywords_status: "observed",
      available_tabs: ["videos"],
      available_tabs_status: "observed",
      external_links: [{ target_url: "https://previous.example" }],
      external_links_status: "observed",
      is_verified: true,
      is_verified_status: "verified",
      joined_at: new Date("2020-01-02T00:00:00.000Z"),
      joined_at_precision: "date_only",
      youtube_business_email_available: true,
      youtube_business_email_observed_at: new Date("2026-07-25T12:00:00.000Z"),
    });

    await pool.query(
      `UPDATE crawler.channels
       SET summary='Summary-only trusted',about_description=NULL,description_status='exact',
           external_links='{"legacy":"shape"}'::jsonb,external_links_status='observed'
       WHERE channel_id=$1`,
      [channelId],
    );
    const legacyClient = await pool.connect();
    try {
      await legacyClient.query("BEGIN");
      await recordAboutObservation(legacyClient, {
        idempotencyKey: `about:legacy-current:${suffix}`,
        channelId,
        observedAt: "2026-07-27T12:00:00.000Z",
        triggerReason: "manual",
        crawlerVersion: "integration-test",
        about,
        current: {
          aboutDescription: null,
          descriptionStatus: "empty",
          externalLinks: [],
          externalLinksStatus: "observed",
          isVerified: null,
          isVerifiedStatus: "unknown",
          youtubeBusinessEmailAvailable: false,
          youtubeBusinessEmailStatus: "not_available",
          keywordsStatus: "observed",
          availableTabsStatus: "observed",
          identity: {
            title: "Current title",
            handle: "@current",
            avatar_url: "https://example.test/avatar.jpg",
            keywords: [],
            available_tabs: [],
            summary: null,
          },
        },
      });
      await legacyClient.query("COMMIT");
    } catch (error) {
      await legacyClient.query("ROLLBACK");
      throw error;
    } finally {
      legacyClient.release();
    }

    const legacyCurrent = (await pool.query(
      `SELECT summary,about_description,description_status,
              external_links,external_links_status,youtube_business_email_available,
              youtube_business_email_observed_at
       FROM crawler.channels WHERE channel_id=$1`,
      [channelId],
    )).rows[0];
    assert.deepEqual(legacyCurrent, {
      summary: "Summary-only trusted",
      about_description: null,
      description_status: "exact",
      external_links: { legacy: "shape" },
      external_links_status: "observed",
      youtube_business_email_available: false,
      youtube_business_email_observed_at: new Date("2026-07-27T12:00:00.000Z"),
    });
  } finally {
    await pool.query("DELETE FROM crawler.channels WHERE channel_id=$1", [channelId]).catch(() => {});
    await pool.end();
  }
});

test("About writer persists confirmed empty fields when no trusted Current exists", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 2 });
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UCaboutempty${suffix}`;
  const about = normalizeAboutMetrics({
    aboutObserved: true,
    metadata: {
      subscriber_count_text: "123 subscribers",
      subscriber_count_source: "youtube_about",
      view_count_text: "4,567 views",
      view_count_source: "youtube_about",
      video_count_text: "89 videos",
      video_count_source: "youtube_about",
    },
  });

  try {
    await pool.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
       VALUES ($1,$2,'Initial empty fields','active')`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await recordAboutObservation(client, {
        idempotencyKey: `about:initial-empty:${suffix}`,
        channelId,
        observedAt: "2026-07-26T12:00:00.000Z",
        triggerReason: "manual",
        crawlerVersion: "integration-test",
        about,
        current: {
          aboutDescription: null,
          descriptionStatus: "empty",
          externalLinks: [],
          externalLinksStatus: "observed",
          isVerified: null,
          isVerifiedStatus: "unknown",
          keywordsStatus: "observed",
          availableTabsStatus: "observed",
          identity: {
            title: "Initial empty fields",
            handle: null,
            avatar_url: null,
            keywords: [],
            available_tabs: [],
            summary: null,
          },
        },
      });
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }

    const current = (await pool.query(
      `SELECT about_description,description_status,keywords,keywords_status,
              available_tabs,available_tabs_status,external_links,external_links_status
       FROM crawler.channels WHERE channel_id=$1`,
      [channelId],
    )).rows[0];
    assert.deepEqual(current, {
      about_description: null,
      description_status: "empty",
      keywords: [],
      keywords_status: "observed",
      available_tabs: [],
      available_tabs_status: "observed",
      external_links: [],
      external_links_status: "observed",
    });
  } finally {
    await pool.query("DELETE FROM crawler.channels WHERE channel_id=$1", [channelId]).catch(() => {});
    await pool.end();
  }
});
