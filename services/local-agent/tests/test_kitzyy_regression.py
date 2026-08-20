import unittest

from qy_channel_profile.contracts import (
    AnalysisPolicy,
    ChannelSnapshot,
    ProfileAnalysisRequest,
)
from qy_channel_profile.processor import ChannelProfileProcessor


KITZYY_CHANNEL_ID = "UCjbXwdi6dpcngo2RG4dJn2g"
def kitzyy_snapshot() -> ChannelSnapshot:
    return ChannelSnapshot.from_mapping({
        "channel": {
            "channel_id": KITZYY_CHANNEL_ID,
            "channel_url": f"https://www.youtube.com/channel/{KITZYY_CHANNEL_ID}",
            "handle": "@Kitzyyy",
            "title": "Kitzyy",
            "country": "Brazil",
            "country_source": "youtube_about",
            "country_code": "BR",
            "country_canonical_name": "Brazil",
            "summary": (
                "jogo videojogos\n\n"
                "For Business Enquiries: kitzyy@hashrateagency.com or "
                "kitzyycontato@gmail.com\n"
                "For Chinese Business: Kitzyy01@163.com"
            ),
            "about_description": (
                "jogo videojogos\n\n"
                "For Business Enquiries: kitzyy@hashrateagency.com or "
                "kitzyycontato@gmail.com\n"
                "For Chinese Business: Kitzyy01@163.com"
            ),
            "keywords": ["kittzy", "kitzy", "kitzyy"],
            "subscriber_count": 539_000,
            "total_view_count": 42_213_964,
            "total_video_count": 129,
        },
        "contents": [
            {
                "source_content_id": "3RBewAWHMEA",
                "content_type": "video",
                "title": (
                    "O ANIME DAS MINA CAVALO "
                    "KKKKKKKKKKKKKKKKKKKKKKKKKKKKK"
                ),
                "description": (
                    "Seja membro deste canal e ganhe benefícios:\n"
                    "https://www.youtube.com/channel/"
                    "UCjbXwdi6dpcngo2RG4dJn2g/join\n\n"
                    "twitter: @IAmKitzyy\n\n"
                    "se quiser ajudar e aparecer no final do vídeo aí, vai "
                    "fazer toda a diferença: livepix.gg/kitzyy\n\n"
                    "00:00 resumo umamusume\n"
                    "16:00 respondendo as perguntas de vcs"
                ),
                "keywords": [
                    "kitzyy",
                    "kitzy",
                    "umamusume",
                    "anime cavalo",
                    "pretty derby",
                    "umamusume pretty derby",
                    "special week",
                    "symboli rudolph",
                    "teio",
                    "haru urara",
                    "silence suzuka",
                    "el condor pasa",
                    "uma musume",
                    "Umamusume: Pretty Derby",
                    "Gold Ship",
                    "Tokai Teio",
                    "Rice Shower",
                    "Oguri Cap",
                    "Agnes Tachyon",
                ],
                "published_at": "2026-07-31T23:42:18Z",
                "first_seen_at": "2026-08-04T10:43:51Z",
                "view_count": 43_515,
                "like_count": 7_922,
                "duration_seconds": 1_628,
            },
            {
                "source_content_id": "yHIlTGChOWQ",
                "content_type": "video",
                "title": (
                    "O ANIME DO GALO FARMADOR DE AURA "
                    "KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK"
                ),
                "description": (
                    "nunca vi um galo com tanta aura na minha vida e olha que "
                    "ele nem é de rinha...\n\n"
                    "muito obrigado por tudo q vcs fizeram por mim desde o "
                    "último vídeo...\n"
                    "caso queira fazer uma pergunta ou me falar algo: "
                    "livepix.gg/kitzyy\n"
                    "pode me mandar um elogio, um xingão, pedir pra eu "
                    "adicionar no discord, na steam, ir call, qualquer coisa "
                    "que vc quiser vc pode mandar pra mim, e de verdade, vai "
                    "fazer muita diferença mesmo, tudo o que vcs tem feito "
                    "pra mim é algo que eu nunca vou esquecer, obrigado por "
                    "tudo, você pode doar desde quanto vc puder até quanto "
                    "quiser, vai fazer toda a diferença\n\n"
                    "Capítulos\n\n"
                    "00:00 resumo do anime\n"
                    "14:03 desabafo no final e atualizações\n\n"
                    "Seja membro deste canal e ganhe benefícios:\n"
                    "https://www.youtube.com/channel/"
                    "UCjbXwdi6dpcngo2RG4dJn2g/join"
                ),
                "keywords": [
                    "rooster fighter",
                    "niwatori fighter",
                    "rooster",
                    "galo",
                    "kokesuke",
                    "elizabeth",
                    "pyoko",
                    "piyoko",
                    "keiji",
                    "galinha",
                    "farmar aura",
                    "aura",
                    "aura anime",
                    "anime",
                    "resumo anime",
                    "resumo rooster fitgher",
                    "resumão",
                    "análise",
                    "aura moments",
                    "kitzyy",
                    "kitzy",
                ],
                "published_at": "2026-07-20T23:32:12Z",
                "first_seen_at": "2026-08-04T10:43:51Z",
                "view_count": 27_475,
                "like_count": 6_262,
                "duration_seconds": 1_139,
            },
        ],
        "as_of": "2026-08-04T11:03:47Z",
        "replay_quality": "approximate_as_of",
    })


class KitzyyRegressionTest(unittest.TestCase):
    def test_portuguese_anime_content_survives_english_business_boilerplate(self):
        snapshot = kitzyy_snapshot()
        processor = ChannelProfileProcessor()
        result = processor.analyze(
            ProfileAnalysisRequest(
                channel_id=snapshot.channel_id,
                input_url=f"https://www.youtube.com/channel/{snapshot.channel_id}",
                as_of=snapshot.as_of,
                policy=AnalysisPolicy.COMPLETE_ESTIMATE,
            ),
            snapshot,
        ).to_dict()

        facts = result["facts"]
        self.assertEqual(facts["country"]["value"], "Brazil")
        self.assertEqual(facts["creator_language"]["value"], "Portuguese")
        self.assertEqual(
            facts["channel_categories"]["value"],
            {
                "level_1": "General Humanities & Society",
                "level_2": ["Arts & Culture"],
            },
        )
        tags = facts["channel_tags"]["value"]["tags"]
        self.assertIn("Anime", tags)
        self.assertIn("Anime Commentary", tags)
        audience_languages = facts["audience_language"]["value"]
        self.assertNotIn(
            "Norwegian",
            {row["language"] for row in audience_languages},
        )
        self.assertEqual(sum(row["percentage"] for row in audience_languages), 100)
        self.assertEqual(
            sum(
                row[gender]
                for row in facts["audience_age_gender"]["value"]
                for gender in ("male", "female")
            ),
            100,
        )
        self.assertEqual(
            sum(
                row["percentage"]
                for row in facts["channel_tags"]["value"]["top_5_distribution"]
            ),
            100,
        )


if __name__ == "__main__":
    unittest.main()
