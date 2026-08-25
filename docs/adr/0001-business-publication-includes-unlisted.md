# Business publication includes Unlisted videos

YouTube Unlisted is a discoverability setting, not an authorization boundary: anyone with the URL can view the video. Business Publication therefore treats `unlisted` like `public` for storage and distribution while preserving the real access status; `private` and `unavailable` remain excluded, and `unknown` or `login_required` remain unproven.

New revisions must not emit `source_unlisted` retractions. Historical revisions and recovery records containing that reason remain readable for compatibility. Existing Unlisted crawler rows need their `publication_item_hash` refreshed before they can enter Business Current.
