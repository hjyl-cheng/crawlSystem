const IDENTITY_LIMITS = Object.freeze({
  about: 800,
  videoCount: 6,
  title: 160,
  description: 180,
  ownerComments: 4,
  commentText: 120,
});

function text(value) {
  const out = String(value ?? "").trim();
  return out || null;
}

function clipIdentityText(value, max) {
  const out = String(value ?? "").replace(/\s+/g, " ").trim();
  if (!out) return "";
  if (out.length <= max) return out;
  return `${out.slice(0, Math.max(0, max - 1)).trimEnd()}…`;
}

function ownerCommentsFromPage(page, remaining) {
  const comments = Array.isArray(page?.comments) ? page.comments : [];
  const selected = [];
  for (const comment of comments) {
    if (selected.length >= remaining) break;
    if (comment?.is_channel_owner !== true) continue;
    const commentText = clipIdentityText(
      comment.text ?? comment.comment_text ?? "",
      IDENTITY_LIMITS.commentText,
    );
    if (!commentText) continue;
    selected.push({
      text: commentText,
      is_pinned: comment.is_pinned === true,
    });
  }
  return selected;
}

export function compactFirstPartyIdentity(raw) {
  if (!raw || typeof raw !== "object") return null;
  const about = clipIdentityText(
    [raw.summary, raw.about_description].filter((value) => text(value)).join("\n"),
    IDENTITY_LIMITS.about,
  );
  const videos = [];
  const ownerComments = [];
  const seenVideoIds = new Set();
  for (const video of Array.isArray(raw.videos) ? raw.videos : []) {
    const videoId = text(video?.source_content_id ?? video?.content_id ?? video?.video_id);
    if (videoId && seenVideoIds.has(videoId)) continue;
    if (videoId) seenVideoIds.add(videoId);
    if (videos.length < IDENTITY_LIMITS.videoCount) {
      const title = clipIdentityText(video?.title, IDENTITY_LIMITS.title);
      const description = clipIdentityText(video?.description, IDENTITY_LIMITS.description);
      if (videoId || title || description) {
        const row = {};
        if (videoId) row.video_id = videoId;
        if (title) row.title = title;
        if (description) row.description = description;
        if (video?.published_at) row.published_at = String(video.published_at);
        videos.push(row);
      }
    }
    if (ownerComments.length < IDENTITY_LIMITS.ownerComments) {
      ownerComments.push(...ownerCommentsFromPage(
        video?.comments_first_page,
        IDENTITY_LIMITS.ownerComments - ownerComments.length,
      ));
    }
  }
  const payload = {};
  if (text(raw.title)) payload.title = clipIdentityText(raw.title, IDENTITY_LIMITS.title);
  if (text(raw.handle)) payload.handle = text(raw.handle);
  if (about) payload.about = about;
  if (videos.length > 0) payload.recent_videos = videos;
  if (ownerComments.length > 0) payload.owner_comments = ownerComments;
  return Object.keys(payload).length > 0 ? payload : null;
}

export function buildFirstPartyIdentityContext(items) {
  const out = {};
  for (const item of items) {
    const compacted = compactFirstPartyIdentity(item?.first_party_identity);
    if (compacted) out[item.input_url] = compacted;
  }
  return out;
}
