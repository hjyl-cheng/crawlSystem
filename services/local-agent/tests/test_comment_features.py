import copy
import unittest

from qy_channel_profile.comment_features import (
    COMMENT_NUMERIC_FEATURE_NAMES,
    build_comment_evidence,
)
from qy_channel_profile.contracts import ChannelSnapshot
from qy_channel_profile.errors import SnapshotError


def comment_page(*comments):
    return {
        "version": 1,
        "collected_at": "2026-08-10T12:00:00Z",
        "sort": "TOP_COMMENTS",
        "total_count": 100,
        "returned_count": len(comments),
        "comments": list(comments),
    }


def comment(comment_id, text, author, *, position=1, **extra):
    return {
        "comment_id": comment_id,
        "position": position,
        "text": text,
        "author_name": author,
        "author_channel_id": f"UC-{author}",
        "author_url": f"https://www.youtube.com/channel/UC-{author}",
        "author_avatar_url": f"https://yt3.ggpht.com/{author}",
        "published_at_utc": "2026-08-09T12:00:00Z",
        "published_text_raw": "1 day ago",
        "published_at_status": "estimated_relative",
        "like_count": extra.get("like_count", 0),
        "reply_count": extra.get("reply_count", 0),
        "is_pinned": extra.get("is_pinned", False),
        "is_channel_owner": extra.get("is_channel_owner", False),
        "is_verified": extra.get("is_verified", False),
        "is_hearted": extra.get("is_hearted", False),
    }


def snapshot_value():
    return {
        "channel": {
            "channel_id": "UCcomment0000000000000001",
            "title": "Canal de Anime",
            "subscriber_count": 100000,
            "total_view_count": 1000000,
            "total_video_count": 20,
        },
        "contents": [
            {
                "source_content_id": "v1",
                "content_type": "video",
                "title": "Anime novo",
                "published_at": "2026-08-08T00:00:00Z",
                "first_seen_at": "2026-08-08T01:00:00Z",
                "view_count": 10000,
                "comment_count": 100,
                "comments_first_page": comment_page(
                    comment("c1", "Assistindo do Brasil, mano. Este anime ficou incrível!", "one", like_count=20),
                    comment("c2", "Sou uma mulher de 27 anos e adoro a análise do mangá.", "two", like_count=5),
                    comment("c3", "Nice video!", "three"),
                    comment("c4", "Bro, você explicou muito bem a história do anime.", "owner", is_channel_owner=True),
                ),
            },
            {
                "source_content_id": "v2",
                "content_type": "video",
                "title": "Análise de mangá",
                "published_at": "2026-08-01T00:00:00Z",
                "first_seen_at": "2026-08-01T01:00:00Z",
                "view_count": 5000,
                "comment_count": 50,
                "comments_first_page": comment_page(
                    comment("c5", "Voltei para ver outra ótima análise de anime, irmão!", "one", reply_count=2),
                    comment("c6", "Eu sou um homem de 34 anos e leio este mangá.", "four"),
                ),
            },
        ],
        "as_of": "2026-08-11T00:00:00Z",
    }


class CommentFeatureTest(unittest.TestCase):
    def test_contract_and_hashes_separate_comment_text_from_comment_stats(self):
        original = ChannelSnapshot.from_mapping(snapshot_value())
        changed_likes = copy.deepcopy(snapshot_value())
        changed_likes["contents"][0]["comments_first_page"]["comments"][0]["like_count"] += 1
        stats_snapshot = ChannelSnapshot.from_mapping(changed_likes)
        self.assertEqual(original.hashes()["comment_text_hash"], stats_snapshot.hashes()["comment_text_hash"])
        self.assertNotEqual(original.hashes()["comment_stats_hash"], stats_snapshot.hashes()["comment_stats_hash"])

        changed_text = copy.deepcopy(snapshot_value())
        changed_text["contents"][0]["comments_first_page"]["comments"][0]["text"] += " novo"
        text_snapshot = ChannelSnapshot.from_mapping(changed_text)
        self.assertNotEqual(original.hashes()["comment_text_hash"], text_snapshot.hashes()["comment_text_hash"])

    def test_avatar_url_is_retained_but_does_not_change_comment_evidence(self):
        original = ChannelSnapshot.from_mapping(snapshot_value())
        changed = copy.deepcopy(snapshot_value())
        changed["contents"][0]["comments_first_page"]["comments"][0][
            "author_avatar_url"
        ] = "https://yt3.ggpht.com/a-different-avatar"
        changed_snapshot = ChannelSnapshot.from_mapping(changed)
        self.assertNotEqual(
            original.contents[0].comments_first_page.comments[0].author_avatar_url,
            changed_snapshot.contents[0].comments_first_page.comments[0].author_avatar_url,
        )
        self.assertEqual(
            build_comment_evidence(original),
            build_comment_evidence(changed_snapshot),
        )

    def test_snapshot_rejects_comment_page_collected_after_as_of(self):
        changed = copy.deepcopy(snapshot_value())
        changed["contents"][0]["comments_first_page"]["collected_at"] = (
            "2026-08-12T00:00:00Z"
        )
        with self.assertRaises(SnapshotError):
            ChannelSnapshot.from_mapping(changed)

    def test_builder_aggregates_unique_authors_and_excludes_owner_from_audience(self):
        evidence = build_comment_evidence(ChannelSnapshot.from_mapping(snapshot_value()))
        self.assertEqual(tuple(evidence.numeric), COMMENT_NUMERIC_FEATURE_NAMES)
        self.assertEqual(evidence.numeric["comment_sample_count"], 6)
        self.assertEqual(evidence.numeric["comment_unique_author_count"], 4)
        self.assertEqual(evidence.numeric["comment_returning_author_count"], 1)
        self.assertEqual(evidence.numeric["comment_owner_ratio"], 1 / 6)
        self.assertEqual(evidence.region_probabilities["Brazil"], 1.0)
        self.assertEqual(evidence.gender_probabilities, {"female": 0.5, "male": 0.5})
        self.assertIn("25-34_female", evidence.age_gender_probabilities)
        self.assertIn("25-34_male", evidence.age_gender_probabilities)
        self.assertIn("anime", evidence.topic_text)
        self.assertNotIn("nice video", evidence.topic_text)
        self.assertNotIn("você explicou muito bem", evidence.topic_text)
        self.assertIn("sample_bias:top_comments", evidence.diagnostics)

    def test_empty_snapshot_returns_explicit_missing_features(self):
        value = snapshot_value()
        for content in value["contents"]:
            content.pop("comments_first_page")
        evidence = build_comment_evidence(ChannelSnapshot.from_mapping(value))
        self.assertFalse(evidence.has_comments)
        self.assertEqual(evidence.numeric["comment_features_missing"], 1.0)
        self.assertEqual(evidence.topic_text, "")

    def test_creator_gender_address_requires_context_and_author_consensus(self):
        ambiguous = snapshot_value()
        ambiguous["contents"] = [ambiguous["contents"][0]]
        ambiguous["contents"][0]["comments_first_page"] = comment_page(
            comment("a1", "Chico Bento apareceu neste episódio do anime.", "one"),
            comment("a2", "Esse rapaz do mangá é um personagem muito estranho.", "two"),
            comment("a3", "A menina da história foi muito bem escrita.", "three"),
            comment("a4", "O mano do anime virou o melhor personagem.", "four"),
        )
        ambiguous_evidence = build_comment_evidence(ChannelSnapshot.from_mapping(ambiguous))
        self.assertEqual(ambiguous_evidence.creator_gender_probabilities, {})

        consensus = snapshot_value()
        consensus["contents"] = [consensus["contents"][0]]
        consensus["contents"][0]["comments_first_page"] = comment_page(
            comment("m1", "Bro, você explicou muito bem este anime.", "one"),
            comment("m2", "Irmão, você sempre faz uma ótima análise.", "two"),
            comment("m3", "Amigo, seu resumo do mangá ficou excelente.", "three"),
        )
        consensus_evidence = build_comment_evidence(ChannelSnapshot.from_mapping(consensus))
        self.assertEqual(consensus_evidence.creator_gender_probabilities, {"male": 1.0})
        self.assertEqual(consensus_evidence.numeric["comment_creator_gender_author_count"], 3)

    def test_creator_name_grammar_consensus_tracks_independent_videos(self):
        value = snapshot_value()
        value["channel"]["title"] = "coruga"
        value["channel"]["handle"] = "@corugaantiroleplay"
        value["contents"] = []
        for video_index in range(3):
            value["contents"].append({
                "source_content_id": f"rp-{video_index}",
                "content_type": "video",
                "title": f"GTA roleplay {video_index}",
                "published_at": f"2026-08-0{8-video_index}T00:00:00Z",
                "comments_first_page": comment_page(*[
                    comment(
                        f"g-{video_index}-{author_index}",
                        f"O coruga jogou muito neste vídeo {author_index}!",
                        f"grammar-{video_index}-{author_index}",
                    )
                    for author_index in range(2)
                ]),
            })

        evidence = build_comment_evidence(ChannelSnapshot.from_mapping(value))

        self.assertEqual(evidence.creator_gender_probabilities, {"male": 1.0})
        self.assertEqual(evidence.creator_gender_support["male"]["author_count"], 6)
        self.assertEqual(evidence.creator_gender_support["male"]["video_count"], 3)

    def test_multiword_channel_title_grammar_is_not_person_gender(self):
        value = snapshot_value()
        value["channel"]["title"] = "Estranha História"
        value["channel"]["handle"] = "@henriquecaldeira"
        value["contents"] = [value["contents"][0]]
        value["contents"][0]["comments_first_page"] = comment_page(
            comment("g1", "A história ficou muito bem explicada.", "one"),
            comment("g2", "A história deste vídeo é fascinante.", "two"),
            comment("g3", "A estranha história merece outra parte.", "three"),
        )

        evidence = build_comment_evidence(ChannelSnapshot.from_mapping(value))

        self.assertEqual(evidence.creator_gender_probabilities, {})
        self.assertEqual(evidence.creator_gender_support, {})


if __name__ == "__main__":
    unittest.main()
