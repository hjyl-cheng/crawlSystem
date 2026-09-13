export function inputFixture() {
  return { version: 1, generation: 1, runId: 'trial-run', cycleKey: 'base',
    observedAt: '2026-09-12T00:00:00Z', anchors: [{ id: 'stored', published_day: '2026-09-01' }],
    apiPolicy: { enabled: true, dailyRequestLimit: 10000 },
    plan: { plan_id: 'trial-plan', channel_id: 'UC-isolated-trial', plan_day: '2026-09-12',
      task_mask: { about: true, video: true, agent: false },
      capacity: { factor: 1, player_cap: 20, next_cap: 8 }, planner_config_version: 'video-plan-1' },
    snapshot: { reservation_model: 'isolated-no-concurrent-enrich',
      knownIds: ['stored'], dispositions: [], pendingFirstSeen: [], dueEntries: [],
      recentRows: [{ content_key: 'stored-key', source_content_id: 'stored',
        published_at: '2026-09-01T00:00:00Z', player_last_observed_at: null }] } };
}

export function detailFixture(id) {
  return { id, title: 'Channel trial', thumbnail_url: 'https://i.ytimg.com/example.jpg',
    published_at: '2026-09-01T00:00:00Z', published_at_status: 'exact', duration_seconds: 61,
    view_count: 10, view_count_text: '10', like_count: 2, comment_count: 0,
    comment_count_status: 'exact', comments_disabled: true,
    comments_first_page: { total_count: 0, returned_count: 0, comments: [] },
    description: '', description_observed: true, hashtags: [], hashtags_observed: true,
    keywords: [], keywords_observed: true, access_status: 'public',
    content_type_signals: { source: 'youtubei_player', is_shorts_eligible: false,
      is_live_content: false, is_live: false, is_upcoming: false, is_live_now: false },
    extractor_version: 'youtubei.js@test', source: 'youtubejs_get_info' };
}

export function youtubeFixture(events = [], { entries = [{ id: 'new-a' }, { id: 'new-b' }, { id: 'stored' }],
  fetchDetail = async id => detailFixture(id), scanPatch = {} } = {}) {
  return {
    openChannel: async (_id, options) => {
      events.push(['about', options.about]);
      return { about_requested: options.about, about_observed: options.about,
        metadata: { title: 'Trial' }, raw: { fixture: true },
        scanUploads: async args => {
          events.push(['scan', args]);
          return { entries, complete: true, stop_reason: 'list_end', terminal_reason: 'list_end',
            pages: 1, parse_gap_count: 0, ...scanPatch };
        } };
    },
    fetchDetail: async (id, options) => { events.push(['detail', id, options.detailMode]); return fetchDetail(id, options); },
  };
}
