import unittest
from datetime import datetime, timezone

from qy_channel_profile.analyzers import (
    analyze_active_ratio,
    analyze_age,
    analyze_audience_age_gender,
    analyze_audience_markets,
    analyze_categories,
    analyze_gender,
    analyze_tags,
)
from qy_channel_profile.comment_features import build_comment_evidence
from qy_channel_profile.contracts import AnalysisPolicy, ChannelSnapshot, FieldResult
from qy_channel_profile.priors import PriorCatalog


def field_result(value, *, strength="strong"):
    return FieldResult(
        value=value,
        source_type="rule_inferred",
        truth_status="estimated",
        evidence_strength=strength,
        model_confidence=0.9,
        evidence_confidence=0.9,
    )


class AnalyzerPrecisionTest(unittest.TestCase):
    def setUp(self):
        self.catalog = PriorCatalog.load()

    def snapshot(self, *, title="Example", about="", contents=None):
        return ChannelSnapshot.from_mapping({
            "channel": {
                "channel_id": "UC1234567890123456789012",
                "title": title,
                "about_description": about,
            },
            "contents": contents or [],
            "as_of": datetime(2026, 8, 9, tzinfo=timezone.utc),
        })

    def test_one_ambiguous_team_word_does_not_pass_evidence_first(self):
        result = analyze_gender(
            self.snapshot(about="Welcome to my official channel."),
            AnalysisPolicy.EVIDENCE_FIRST,
        )
        self.assertIsNone(result.value)
        self.assertTrue(result.abstained)

    def test_agency_inside_business_contact_does_not_make_channel_a_team(self):
        result = analyze_gender(
            self.snapshot(
                title="Kitzyy",
                about="For business enquiries: creator@hashrateagency.com",
            ),
            AnalysisPolicy.EVIDENCE_FIRST,
        )
        self.assertIsNone(result.value)
        self.assertTrue(result.abstained)

    def test_explicit_creator_identity_and_age_are_extracted_from_profile_only(self):
        snapshot = self.snapshot(
            title="Canal de Lucia",
            about="Soy una creadora y tengo 27 años. Bienvenidos a mi canal.",
        )
        gender = analyze_gender(snapshot, AnalysisPolicy.EVIDENCE_FIRST)
        age = analyze_age(snapshot, "Casual Vlogs", self.catalog, AnalysisPolicy.EVIDENCE_FIRST)
        self.assertEqual(gender.value, "female")
        self.assertEqual(gender.evidence_strength, "strong")
        self.assertEqual(age.value, 27)
        self.assertEqual(age.evidence_strength, "explicit")

    def test_explicit_single_creator_attribution_overrides_weak_official_marker(self):
        male = analyze_gender(
            self.snapshot(
                title="Professor Caio",
                about="Canal oficial criado pelo Prof Caio Ribeiro.",
            ),
            AnalysisPolicy.EVIDENCE_FIRST,
        )
        female = analyze_gender(
            self.snapshot(
                title="Cozinha da Lia",
                about="Canal oficial criado por uma chef chamada Lia.",
            ),
            AnalysisPolicy.EVIDENCE_FIRST,
        )
        self.assertEqual(male.value, "male")
        self.assertEqual(female.value, "female")
        self.assertEqual(male.evidence_strength, "strong")
        self.assertEqual(female.evidence_strength, "strong")

    def test_brand_channel_can_have_an_explicit_female_primary_creator(self):
        snapshot = self.snapshot(
            title="Beleza Ruiva Oficial",
            about=(
                "Olá, muito prazer eu me chamo Aline Castro sou CEO da Beleza Ruiva, "
                "a maior loja para ruivas do Brasil!"
            ),
            contents=[{
                "source_content_id": f"beauty-{index}",
                "content_type": "short",
                "title": f"Cuidados para cabelos ruivos {index}",
                "description": (
                    "A fundadora da Beleza Ruiva trouxe uma dica importante.\n\n"
                    "// SOBRE MIM:\n"
                    "Olá, eu me chamo Aline Castro sou CEO da Beleza Ruiva."
                ),
                "published_at": f"2026-08-0{8-index}T00:00:00Z",
            } for index in range(3)],
        )

        result = analyze_gender(snapshot, AnalysisPolicy.EVIDENCE_FIRST)

        self.assertEqual(result.value, "female")
        self.assertEqual(result.evidence_strength, "strong")
        self.assertEqual(result.metadata["account_entity_type"], "brand_or_team")
        self.assertEqual(result.metadata["primary_creator_status"], "single_stable")
        self.assertEqual(result.metadata["primary_creator_subject"], "Aline Castro")
        self.assertEqual(result.metadata["evidence_tier"], "A")
        self.assertLessEqual(len(result.evidence_refs), 3)
        self.assertGreater(result.metadata["deduplicated_evidence_count"], 0)

    def test_named_founder_in_another_video_still_binds_primary_gender(self):
        snapshot = self.snapshot(
            title="Beleza Ruiva Oficial",
            about=(
                "Olá, muito prazer eu me chamo Aline Castro sou CEO da Beleza Ruiva, "
                "a maior loja para ruivas do Brasil!"
            ),
            contents=[
                {
                    "source_content_id": "product-review",
                    "content_type": "short",
                    "title": "Tonalizante Alegria",
                    "description": "Comparativo de tonalizantes para cabelos ruivos.",
                    "published_at": "2026-08-08T00:00:00Z",
                },
                {
                    "source_content_id": "founder-alert",
                    "content_type": "short",
                    "title": "Novidades de outubro",
                    "description": (
                        "A fundadora da Beleza Ruiva trouxe um alerta importante "
                        "para você não perder os lançamentos."
                    ),
                    "published_at": "2026-08-07T00:00:00Z",
                },
            ],
        )

        result = analyze_gender(snapshot, AnalysisPolicy.EVIDENCE_FIRST)

        self.assertEqual(result.value, "female")
        self.assertEqual(result.metadata["account_entity_type"], "brand_or_team")
        self.assertEqual(result.metadata["primary_creator_status"], "single_stable")
        self.assertEqual(result.metadata["primary_creator_subject"], "Aline Castro")
        self.assertIn("explicit_role_attribution", result.metadata["evidence_claim_types"])

    def test_guest_founder_mention_is_not_primary_creator_gender(self):
        snapshot = self.snapshot(
            title="Podcast Semanal",
            about="Entrevistas com empreendedoras.",
            contents=[{
                "source_content_id": "guest-founder",
                "content_type": "video",
                "title": "Entrevista",
                "description": "Hoje a convidada é a fundadora da marca X, que explica o negócio.",
                "published_at": "2026-08-08T00:00:00Z",
            }],
        )

        result = analyze_gender(snapshot, AnalysisPolicy.EVIDENCE_FIRST)

        self.assertIsNone(result.value)
        self.assertTrue(result.abstained)

    def test_video_guest_role_is_not_primary_creator_gender(self):
        snapshot = self.snapshot(
            title="maju trindade",
            about="majutrindade.com",
            contents=[{
                "source_content_id": "guest-role",
                "content_type": "video",
                "title": "Podcast about purpose",
                "description": (
                    "The introduction was recited by a content creator "
                    "called Zach. Follow the channel for more episodes."
                ),
                "published_at": "2026-08-08T00:00:00Z",
            }],
        )

        result = analyze_gender(snapshot, AnalysisPolicy.EVIDENCE_FIRST)

        self.assertIsNone(result.value)
        self.assertTrue(result.abstained)

    def test_owner_comment_self_identification_is_creator_evidence(self):
        snapshot = self.snapshot(contents=[{
            "source_content_id": "owner-comment",
            "content_type": "video",
            "title": "Perguntas e respostas",
            "comments_first_page": {
                "version": 1,
                "collected_at": "2026-08-08T12:00:00Z",
                "sort": "TOP_COMMENTS",
                "returned_count": 1,
                "comments": [{
                    "comment_id": "owner-1",
                    "position": 1,
                    "text": "Sou uma mulher e a criadora deste canal.",
                    "author_channel_id": "UC1234567890123456789012",
                    "is_channel_owner": True,
                    "is_pinned": True,
                    "published_at": "2026-08-08T10:00:00Z",
                }],
            },
        }])

        result = analyze_gender(snapshot, AnalysisPolicy.EVIDENCE_FIRST)

        self.assertEqual(result.value, "female")
        self.assertEqual(result.metadata["evidence_tier"], "A")
        self.assertIn("owner_comment", result.evidence_refs[0])

    def test_sou_a_name_and_feminine_profession_is_female(self):
        result = analyze_gender(
            self.snapshot(
                title="Hérika Fagundes",
                about="Oie, eu sou a Héri! Maquiadora e criadora de conteúdo apaixonada por ensinar.",
            ),
            AnalysisPolicy.EVIDENCE_FIRST,
        )
        self.assertEqual(result.value, "female")
        self.assertEqual(result.evidence_strength, "strong")

    def test_criadora_and_fundadora_without_sou_uma_is_female(self):
        result = analyze_gender(
            self.snapshot(
                title="Sabrina Nunes",
                about="Eu sou Sabrina, Criadora da Francisca Joias e fundadora de uma marca de joias.",
            ),
            AnalysisPolicy.EVIDENCE_FIRST,
        )
        self.assertEqual(result.value, "female")

    def test_sou_o_name_is_male(self):
        result = analyze_gender(
            self.snapshot(
                title="Márcio Ribeiro",
                about="Oi! Eu sou o Márcio e nesse canal vou compartilhar dicas de compras.",
            ),
            AnalysisPolicy.EVIDENCE_FIRST,
        )
        self.assertEqual(result.value, "male")

    def test_gendered_adjective_and_father_role_are_self_identification(self):
        female = analyze_gender(
            self.snapshot(
                title="MinimaLista por Luisa Velasco",
                about="Olá, meu nome é Luisa Velasco! Sou apaixonada pela estética minimalista.",
            ),
            AnalysisPolicy.EVIDENCE_FIRST,
        )
        male = analyze_gender(
            self.snapshot(
                title="Patricio Carvalho",
                about="Um pai alimentando sua família. Ensino comida que brasileiro ama.",
            ),
            AnalysisPolicy.EVIDENCE_FIRST,
        )
        self.assertEqual(female.value, "female")
        self.assertEqual(male.value, "male")

    def test_podcast_or_tv_in_title_is_not_enough_to_call_the_channel_a_team(self):
        podcast = analyze_gender(
            self.snapshot(title="Splash and Go Podcast", about="Racing news and analysis with emphasis in F1."),
            AnalysisPolicy.EVIDENCE_FIRST,
        )
        tv = analyze_gender(
            self.snapshot(title="TV MESTRE JOSÉ", about="Um canal voltado ao misticismo e previsões."),
            AnalysisPolicy.EVIDENCE_FIRST,
        )
        self.assertTrue(podcast.abstained)
        self.assertTrue(tv.abstained)

    def test_quem_somos_and_domain_club_are_not_team_markers(self):
        films = analyze_gender(
            self.snapshot(
                title="Adriano Lo Sardo",
                about="LO SARDO FILMS. Viajar muda e muito quem somos.",
            ),
            AnalysisPolicy.EVIDENCE_FIRST,
        )
        club = analyze_gender(
            self.snapshot(
                title="Lucas Montano",
                about="Lead Engineer no Disney+. Comunidade: https://stupidbutton.club",
            ),
            AnalysisPolicy.EVIDENCE_FIRST,
        )
        self.assertTrue(films.abstained)
        self.assertTrue(club.abstained)

    def test_title_name_after_honorific_is_complete_estimate_only(self):
        snapshot = self.snapshot(
            title="Dr. David Gusmão - Hip Surgery",
            about="Focused on the hip joint and cartilage.",
        )
        estimated = analyze_gender(snapshot, AnalysisPolicy.COMPLETE_ESTIMATE)
        withheld = analyze_gender(snapshot, AnalysisPolicy.EVIDENCE_FIRST)
        self.assertEqual(estimated.value, "male")
        self.assertEqual(estimated.metadata["evidence_tier"], "C")
        self.assertTrue(withheld.abstained)

    def test_name_after_por_or_com_is_used_in_complete_estimate(self):
        luisa = analyze_gender(
            self.snapshot(title="MinimaLista por Luisa Velasco", about="Canal de organização e estilo."),
            AnalysisPolicy.COMPLETE_ESTIMATE,
        )
        gabi = analyze_gender(
            self.snapshot(title="Tratando de Estetica com Gabi Tuller", about="Pesquisas e prática em estética."),
            AnalysisPolicy.COMPLETE_ESTIMATE,
        )
        self.assertEqual(luisa.value, "female")
        self.assertEqual(gabi.value, "female")

    def test_religious_o_criador_is_not_primary_creator_gender(self):
        snapshot = self.snapshot(
            title="Chico Xavier Caridade e Luz",
            about="Eu sou Ronaldo, médium espírita do templo.",
            contents=[{
                "source_content_id": "prayer",
                "content_type": "video",
                "title": "Oração",
                "comments_first_page": {
                    "version": 1,
                    "collected_at": "2026-08-08T12:00:00Z",
                    "sort": "TOP_COMMENTS",
                    "returned_count": 1,
                    "comments": [{
                        "comment_id": "owner-god",
                        "position": 1,
                        "text": "Glória ao Criador de tudo.",
                        "author_channel_id": "UC1234567890123456789012",
                        "is_channel_owner": True,
                        "published_at": "2026-08-08T10:00:00Z",
                    }],
                },
            }],
        )
        result = analyze_gender(snapshot, AnalysisPolicy.EVIDENCE_FIRST)
        self.assertIsNone(result.value)
        self.assertTrue(result.abstained)

    def test_story_character_dona_is_not_primary_creator_gender(self):
        snapshot = self.snapshot(
            title="Sabedoria Divina",
            about="Histórias para reflexão e autoconhecimento.",
            contents=[{
                "source_content_id": "story-dona",
                "content_type": "video",
                "title": "Nem Toda Porta Aberta Vem de Deus",
                "description": (
                    "Acompanhe a jornada de Sílvia, uma dona de confecção que confundiu "
                    "o desejo de crescer com a direção de Deus."
                ),
                "published_at": "2026-08-08T00:00:00Z",
            }],
        )
        result = analyze_gender(snapshot, AnalysisPolicy.EVIDENCE_FIRST)
        self.assertIsNone(result.value)
        self.assertTrue(result.abstained)

    def test_criador_de_conteudo_is_male(self):
        result = analyze_gender(
            self.snapshot(title="variedades de vídeos", about="Criador de conteúdo. Variedades Jardins."),
            AnalysisPolicy.EVIDENCE_FIRST,
        )
        self.assertEqual(result.value, "male")

    def test_renamed_channel_uses_previous_given_name_in_complete_estimate(self):
        snapshot = self.snapshot(
            title="Maquininha Correta",
            about="O canal Claudio Oliveira agora é Maquininha Correta. O nome mudou, o conteúdo continua.",
        )
        estimated = analyze_gender(snapshot, AnalysisPolicy.COMPLETE_ESTIMATE)
        withheld = analyze_gender(snapshot, AnalysisPolicy.EVIDENCE_FIRST)
        self.assertEqual(estimated.value, "male")
        self.assertEqual(estimated.metadata["evidence_tier"], "C")
        self.assertTrue(withheld.abstained)

    def test_brand_without_named_primary_does_not_use_comment_gender(self):
        contents = []
        for video_index in range(3):
            contents.append({
                "source_content_id": f"news-{video_index}",
                "content_type": "video",
                "title": f"Review {video_index}",
                "published_at": f"2026-08-0{8-video_index}T00:00:00Z",
                "comments_first_page": {
                    "version": 1,
                    "collected_at": "2026-08-08T12:00:00Z",
                    "sort": "TOP_COMMENTS",
                    "returned_count": 2,
                    "comments": [{
                        "comment_id": f"male-{video_index}-{author_index}",
                        "position": author_index + 1,
                        "text": f"Mano, esse cara mandou bem no review {video_index}-{author_index}!",
                        "author_channel_id": f"UCmale{video_index}{author_index}",
                        "is_channel_owner": False,
                    } for author_index in range(2)],
                },
            })
        snapshot = self.snapshot(
            title="Flow Games",
            about="Notícias, reviews e esports. Contato comercial: comercial@grupoflow.media",
            contents=contents,
        )
        comments = build_comment_evidence(snapshot)
        self.assertTrue(comments.creator_gender_probabilities)

        result = analyze_gender(
            snapshot,
            AnalysisPolicy.COMPLETE_ESTIMATE,
            comments=comments,
        )

        self.assertEqual(result.value, "brand_team")
        self.assertNotEqual(result.source_type, "public_signal_model")

    def test_historical_content_age_is_advanced_to_snapshot_date(self):
        snapshot = self.snapshot(contents=[{
            "source_content_id": "old-about-me",
            "content_type": "video",
            "title": "My birthday vlog",
            "description": "// ABOUT ME: I am a 25-year-old creator.",
            "published_at": "2023-08-09T00:00:00Z",
        }])

        result = analyze_age(
            snapshot,
            "Casual Vlogs",
            self.catalog,
            AnalysisPolicy.EVIDENCE_FIRST,
        )

        self.assertEqual(result.value, 28)
        self.assertEqual(result.evidence_strength, "explicit")
        self.assertEqual(result.metadata["age_interval"], [28, 28])
        self.assertEqual(result.metadata["evidence_basis"], "explicit_age")
        self.assertIn("content:old-about-me", result.evidence_refs[0])

    def test_first_person_age_in_another_video_binds_to_named_creator(self):
        snapshot = self.snapshot(
            title="Casinha da Nati",
            about="Oi gente. Me chamo Natália mas pode me chamar de nati. Sou casada e tenho 3 filhos.",
            contents=[{
                "source_content_id": "diet-vlog",
                "content_type": "video",
                "title": "Vlog da dieta",
                "description": "📌 Tenho 40 anos\n📌 1,49 m de altura\n📌 Peso inicial: 90,2 kg",
                "published_at": "2026-07-24T21:14:19Z",
            }],
        )

        result = analyze_age(
            snapshot,
            "Casual Vlogs",
            self.catalog,
            AnalysisPolicy.EVIDENCE_FIRST,
        )

        self.assertEqual(result.value, 40)
        self.assertEqual(result.evidence_strength, "explicit")
        self.assertEqual(result.metadata["evidence_basis"], "explicit_age")
        self.assertIn("content:diet-vlog", result.evidence_refs[0])

    def test_personal_info_birth_year_in_video_bio_is_creator_age(self):
        snapshot = self.snapshot(
            title="Jesus é a Verdade",
            about="Canal pessoal e evangelístico, conduzido por um obreiro da fé pentecostal.",
            contents=[{
                "source_content_id": "oracao",
                "content_type": "live",
                "title": "Oração pentecostal",
                "description": (
                    ".... INFORMAÇÕES PESSOAIS MINISTÉRIO....\n"
                    "Evangelista Elismar Dias\n"
                    "Nascido em 1976, casado com Luciane."
                ),
                "published_at": "2026-08-08T20:55:06Z",
            }],
        )

        result = analyze_age(
            snapshot,
            "General Humanities & Society",
            self.catalog,
            AnalysisPolicy.EVIDENCE_FIRST,
        )

        self.assertEqual(result.value, 50)
        self.assertEqual(result.metadata["evidence_basis"], "birth_year")
        self.assertEqual(result.metadata["age_interval"], [49, 50])

    def test_video_guest_age_and_birth_year_are_not_creator_evidence(self):
        snapshot = self.snapshot(contents=[{
            "source_content_id": "guest-interview",
            "content_type": "video",
            "title": "Interview with Ana",
            "description": (
                "Our guest Ana says: I am 25 years old. "
                "She was born in 2001 and explains her career."
            ),
            "published_at": "2026-08-08T00:00:00Z",
        }])

        result = analyze_age(
            snapshot,
            "People & Society",
            self.catalog,
            AnalysisPolicy.EVIDENCE_FIRST,
        )

        self.assertIsNone(result.value)
        self.assertTrue(result.abstained)

    def test_channel_profile_reported_guest_age_is_not_creator_evidence(self):
        snapshot = self.snapshot(
            about="Interviews with artists. Guest Ana says: I am 25 years old.",
        )

        result = analyze_age(
            snapshot,
            "People & Society",
            self.catalog,
            AnalysisPolicy.EVIDENCE_FIRST,
        )

        self.assertIsNone(result.value)
        self.assertTrue(result.abstained)

    def test_comment_consensus_uses_winning_candidate_support_count(self):
        contents = []
        for video_index in range(3):
            comments = [{
                "comment_id": f"male-{video_index}-{author_index}",
                "position": author_index + 1,
                "text": (
                    f"O coruga jogou muito bem neste vídeo "
                    f"{video_index}-{author_index}!"
                ),
                "author_channel_id": f"UCmale{video_index}{author_index}",
                "is_channel_owner": False,
            } for author_index in range(2)]
            if video_index == 0:
                comments.append({
                    "comment_id": "female-minority",
                    "position": 3,
                    "text": "A coruga jogou muito bem neste vídeo!",
                    "author_channel_id": "UCfemale",
                    "is_channel_owner": False,
                })
            contents.append({
                "source_content_id": f"rp-{video_index}",
                "content_type": "video",
                "title": f"GTA roleplay {video_index}",
                "published_at": f"2026-08-0{8-video_index}T00:00:00Z",
                "comments_first_page": {
                    "version": 1,
                    "collected_at": "2026-08-08T12:00:00Z",
                    "sort": "TOP_COMMENTS",
                    "returned_count": len(comments),
                    "comments": comments,
                },
            })
        snapshot = self.snapshot(title="coruga", contents=contents)
        comments = build_comment_evidence(snapshot)

        result = analyze_gender(
            snapshot,
            AnalysisPolicy.EVIDENCE_FIRST,
            comments=comments,
        )

        self.assertEqual(result.value, "male")
        self.assertTrue(result.metadata["consensus_gate_passed"])
        self.assertEqual(result.metadata["support"]["author_count"], 6)
        self.assertIn("comment_creator_address_authors:6", result.evidence_refs)

    def test_category_requires_repeated_content_coverage_for_strong_evidence(self):
        one = self.snapshot(contents=[{
            "source_content_id": "game-1",
            "content_type": "video",
            "title": "Roblox gameplay challenge",
            "published_at": "2026-08-08T00:00:00Z",
            "view_count": 1000,
        }])
        three = self.snapshot(contents=[{
            "source_content_id": f"game-{index}",
            "content_type": "video",
            "title": f"Roblox gameplay challenge {index}",
            "published_at": f"2026-08-0{8-index}T00:00:00Z",
            "view_count": 1000,
        } for index in range(3)])
        weak, _ = analyze_categories(one, AnalysisPolicy.COMPLETE_ESTIMATE)
        strong, _ = analyze_categories(three, AnalysisPolicy.COMPLETE_ESTIMATE)
        self.assertEqual(weak.value["level_1"], "Gaming")
        self.assertEqual(weak.evidence_strength, "weak")
        self.assertEqual(strong.evidence_strength, "strong")

    def test_android_gameplay_is_gaming_not_mobile_tech(self):
        snapshot = self.snapshot(
            title="Android Gamer",
            about="Contato Comercial: suporteandroidgamer@outlook.com",
            contents=[
                {
                    "source_content_id": "valorant",
                    "content_type": "video",
                    "title": "Como BAIXAR e JOGAR o Valorant Mobile em 2026",
                    "published_at": "2026-08-08T00:00:00Z",
                    "view_count": 1000,
                },
                {
                    "source_content_id": "gameplay",
                    "content_type": "video",
                    "title": "JOGUEI o NOVO MONSTER HUNTER - PRIMEIRA GAMEPLAY",
                    "published_at": "2026-08-07T00:00:00Z",
                    "view_count": 1000,
                },
                {
                    "source_content_id": "rust",
                    "content_type": "video",
                    "title": "NOVOS JOGOS para ANDROID E IOS em AGOSTO",
                    "published_at": "2026-08-06T00:00:00Z",
                    "view_count": 1000,
                },
            ],
        )
        result, _ = analyze_categories(snapshot, AnalysisPolicy.COMPLETE_ESTIMATE)
        self.assertEqual(result.value["level_1"], "Gaming")
        self.assertIn(result.value["level_2"][0], {"Action Games", "Casual Games", "Mobile Games"})

    def test_english_pronunciation_is_not_pets_or_home_tools(self):
        snapshot = self.snapshot(
            title="Clear English Academy with Keenyn Rhodes",
            about="I’m Keenyn Rhodes, a speech-language pathologist. How to pronounce the R sound.",
            contents=[{
                "source_content_id": "r-sound",
                "content_type": "video",
                "title": "How to Pronounce the R Sound in English",
                "description": "Practice tools for a clear English R sound.",
                "published_at": "2026-08-08T00:00:00Z",
                "view_count": 1000,
            }],
        )
        result, _ = analyze_categories(snapshot, AnalysisPolicy.COMPLETE_ESTIMATE)
        self.assertNotEqual(result.value["level_1"], "Pets & Animals")
        self.assertNotEqual(result.value["level_1"], "Home")
        self.assertIn(result.value["level_1"], {"Education", "Self Improvement"})

    def test_construction_industry_is_not_swimming(self):
        snapshot = self.snapshot(
            title="Câmara Brasileira da Indústria da Construção - CBIC",
            about="Canal oficial da Câmara Brasileira da Indústria da Construção (CBIC)",
        )
        result, _ = analyze_categories(snapshot, AnalysisPolicy.COMPLETE_ESTIMATE)
        self.assertNotEqual(result.value["level_1"], "Sports & Outdoors")
        self.assertEqual(result.value["level_1"], "Home")

    def test_steam_achievements_are_gaming_not_mobile_tech(self):
        snapshot = self.snapshot(
            title="Shift Tab - Conquistas na Steam",
            about="Canal pra falar de platina, 100% e conquistas na Steam porque o importante é jogar!",
        )
        result, _ = analyze_categories(snapshot, AnalysisPolicy.COMPLETE_ESTIMATE)
        self.assertEqual(result.value["level_1"], "Gaming")

    def test_nursery_rhymes_are_music_not_parenting(self):
        snapshot = self.snapshot(
            title="Kiwi Kids - Nursery Rhymes & Kids Songs",
            about="The happiest place for nursery rhymes, kids’ songs, learning, and fun adventures!",
            contents=[{
                "source_content_id": "bath",
                "content_type": "video",
                "title": "Bath Time Do Do Do | VIRAL Kids Bath Song",
                "published_at": "2026-08-08T00:00:00Z",
                "view_count": 1000,
            }],
        )
        result, _ = analyze_categories(snapshot, AnalysisPolicy.COMPLETE_ESTIMATE)
        self.assertEqual(result.value["level_1"], "Music")

    def test_f1_racing_news_is_sports(self):
        snapshot = self.snapshot(
            title="Splash and Go Podcast",
            about="Racing news and analysis with emphasis in F1.",
        )
        result, _ = analyze_categories(snapshot, AnalysisPolicy.COMPLETE_ESTIMATE)
        self.assertEqual(result.value["level_1"], "Sports & Outdoors")

    def test_f1_in_a_personal_title_is_automotive(self):
        snapshot = self.snapshot(title="Pedro Cerqueira F1", about="")
        result, _ = analyze_categories(snapshot, AnalysisPolicy.COMPLETE_ESTIMATE)
        self.assertEqual(result.value["level_1"], "Automotive")

    def test_animated_movie_clips_are_not_pets(self):
        snapshot = self.snapshot(
            title="Boxoffice Movie Scenes ANIMATION",
            about="The best family and animated films clips and cult scenes.",
            contents=[{
                "source_content_id": "rabbit",
                "content_type": "video",
                "title": "The rabbit and the birds save the kingdom",
                "published_at": "2026-08-08T00:00:00Z",
                "view_count": 1000,
            }],
        )
        result, _ = analyze_categories(snapshot, AnalysisPolicy.COMPLETE_ESTIMATE)
        self.assertNotEqual(result.value["level_1"], "Pets & Animals")

    def test_parish_channel_is_not_parenting_from_mae(self):
        snapshot = self.snapshot(
            title="Paróquia Nossa Senhora do Brasil",
            about="Canal da Paróquia Nossa Senhora do Brasil. Homilia e missa ao vivo.",
        )
        result, _ = analyze_categories(snapshot, AnalysisPolicy.COMPLETE_ESTIMATE)
        self.assertNotEqual(result.value["level_1"], "Parenting")
        self.assertEqual(result.value["level_1"], "General Humanities & Society")

    def test_physical_education_is_not_k12_from_the_word_educacao(self):
        snapshot = self.snapshot(
            title="Educação Física com Vinho",
            about="Canal de educação física e movimento.",
            contents=[{
                "source_content_id": "treino",
                "content_type": "video",
                "title": "Treino e checklist na área da saúde",
                "published_at": "2026-08-08T00:00:00Z",
                "view_count": 1000,
            }],
        )
        result, _ = analyze_categories(snapshot, AnalysisPolicy.COMPLETE_ESTIMATE)
        self.assertNotEqual(result.value["level_1"], "Education")
        self.assertNotIn("K-12 Education", result.value["level_2"])

    def test_joint_audience_model_does_not_copy_creator_language(self):
        snapshot = self.snapshot(
            about="I am based in Brazil and publish global technology videos.",
            contents=[{
                "source_content_id": "video-1",
                "content_type": "video",
                "title": "English technology review",
                "published_at": "2026-08-08T00:00:00Z",
                "view_count": 1000,
            }],
        )
        region, language = analyze_audience_markets(
            {"English": 1.0},
            field_result("Brazil", strength="explicit"),
            self.catalog,
            AnalysisPolicy.COMPLETE_ESTIMATE,
            snapshot=snapshot,
            category=field_result({"level_1": "Tech", "level_2": ["Tech News"]}),
        )
        languages = {row["language"] for row in language.value}
        self.assertIn("English", languages)
        self.assertIn("Portuguese", languages)
        self.assertEqual(sum(row["percentage"] for row in region.value), 100)
        self.assertEqual(sum(row["percentage"] for row in language.value), 100)

    def _male_share(self, result):
        return sum(row["male"] for row in result.value)

    def test_audience_age_gender_is_not_fifty_fifty_for_common_categories(self):
        vlogs = analyze_audience_age_gender(
            field_result({"level_1": "Casual Vlogs", "level_2": ["Lifestyle Vlogs"]}),
            self.catalog,
            AnalysisPolicy.COMPLETE_ESTIMATE,
        )
        music = analyze_audience_age_gender(
            field_result({"level_1": "Music", "level_2": ["Singing"]}),
            self.catalog,
            AnalysisPolicy.COMPLETE_ESTIMATE,
        )
        fashion = analyze_audience_age_gender(
            field_result({"level_1": "Fashion", "level_2": ["Makeup"]}),
            self.catalog,
            AnalysisPolicy.COMPLETE_ESTIMATE,
        )
        gaming = analyze_audience_age_gender(
            field_result({"level_1": "Gaming", "level_2": ["Adventure Games"]}),
            self.catalog,
            AnalysisPolicy.COMPLETE_ESTIMATE,
        )
        self.assertNotEqual(self._male_share(vlogs), 50)
        self.assertGreater(self._male_share(music), self._male_share(vlogs))
        self.assertGreater(self._male_share(gaming), self._male_share(fashion))
        self.assertLess(self._male_share(fashion), 40)
        self.assertEqual(sum(row["male"] + row["female"] for row in vlogs.value), 100)

    def test_known_creator_gender_tilts_audience_gender(self):
        category = field_result({"level_1": "Casual Vlogs", "level_2": ["Lifestyle Vlogs"]})
        female_creator = field_result("female")
        male_creator = field_result("male")
        female_audience = analyze_audience_age_gender(
            category,
            self.catalog,
            AnalysisPolicy.COMPLETE_ESTIMATE,
            creator_gender=female_creator,
        )
        male_audience = analyze_audience_age_gender(
            category,
            self.catalog,
            AnalysisPolicy.COMPLETE_ESTIMATE,
            creator_gender=male_creator,
        )
        self.assertLess(self._male_share(female_audience), self._male_share(male_audience))
        self.assertLess(self._male_share(female_audience), 40)
        self.assertGreater(self._male_share(male_audience), 55)
        self.assertEqual(female_audience.metadata["applied_creator_gender"], "female")

    def test_content_format_changes_audience_age_gender_prior(self):
        def format_snapshot(content_type):
            return self.snapshot(contents=[{
                "source_content_id": f"{content_type}-{index}",
                "content_type": content_type,
                "title": f"Roblox gameplay {index}",
                "duration_seconds": 30 if content_type == "short" else 600,
                "published_at": f"2026-08-0{8-index}T00:00:00Z",
                "view_count": 1000,
            } for index in range(3)])

        category = field_result({"level_1": "Gaming", "level_2": ["Casual Games"]})
        short = analyze_audience_age_gender(
            category,
            self.catalog,
            AnalysisPolicy.COMPLETE_ESTIMATE,
            snapshot=format_snapshot("short"),
        )
        longform = analyze_audience_age_gender(
            category,
            self.catalog,
            AnalysisPolicy.COMPLETE_ESTIMATE,
            snapshot=format_snapshot("video"),
        )
        short_young = short.value[0]["male"] + short.value[0]["female"]
        long_young = longform.value[0]["male"] + longform.value[0]["female"]
        self.assertGreater(short_young, long_young)

    def test_content_format_analyzers_use_source_type_not_duration(self):
        def typed_snapshot(duration_seconds):
            value = self.snapshot(contents=[{
                "source_content_id": f"video-{index}",
                "content_type": "video",
                "title": f"Stable format episode {index}",
                "duration_seconds": duration_seconds,
                "published_at": f"2026-08-0{8-index}T00:00:00Z",
                "view_count": 10_000,
                "like_count": 500,
                "comment_count": 50,
            } for index in range(5)])
            value.channel["subscriber_count"] = 100_000
            return value

        brief = typed_snapshot(30)
        long = typed_snapshot(600)
        category = field_result({"level_1": "Gaming", "level_2": ["Casual Games"]})

        brief_demographics = analyze_audience_age_gender(
            category,
            self.catalog,
            AnalysisPolicy.COMPLETE_ESTIMATE,
            snapshot=brief,
        )
        long_demographics = analyze_audience_age_gender(
            category,
            self.catalog,
            AnalysisPolicy.COMPLETE_ESTIMATE,
            snapshot=long,
        )
        self.assertEqual(brief_demographics.value, long_demographics.value)

        brief_tags = analyze_tags(
            brief,
            category,
            field_result("English"),
            AnalysisPolicy.COMPLETE_ESTIMATE,
        )
        self.assertIn("Long-form Video", brief_tags.metadata["supported_tags"])
        self.assertNotIn("Short-form Video", brief_tags.metadata["supported_tags"])

        brief_active = analyze_active_ratio(
            brief,
            self.catalog,
            AnalysisPolicy.COMPLETE_ESTIMATE,
        )
        long_active = analyze_active_ratio(
            long,
            self.catalog,
            AnalysisPolicy.COMPLETE_ESTIMATE,
        )
        expected_shares = {"short": 0.0, "longform": 1.0, "live": 0.0}
        self.assertEqual(brief_active.metadata["content_format_shares"], expected_shares)
        self.assertEqual(brief_active.value, long_active.value)

    def test_active_ratio_is_continuous_proxy_and_responds_to_views(self):
        def active_snapshot(view_count):
            return self.snapshot(contents=[{
                "source_content_id": f"video-{index}",
                "content_type": "video",
                "title": f"Unique topic episode {index}",
                "published_at": f"2026-08-0{8-index}T00:00:00Z",
                "view_count": view_count,
                "like_count": max(1, view_count // 30),
                "comment_count": max(1, view_count // 400),
            } for index in range(5)])

        low_value = active_snapshot(1_000)
        high_value = active_snapshot(20_000)
        low_value.channel["subscriber_count"] = 100_000
        high_value.channel["subscriber_count"] = 100_000
        low = analyze_active_ratio(low_value, self.catalog, AnalysisPolicy.COMPLETE_ESTIMATE)
        high = analyze_active_ratio(high_value, self.catalog, AnalysisPolicy.COMPLETE_ESTIMATE)
        self.assertGreater(high.value, low.value)
        self.assertNotIn("anchor_range", high.metadata)
        self.assertIn("empirical_bayes_evidence_weight", high.metadata)

    def test_tag_attribution_uses_word_boundaries(self):
        snapshot = self.snapshot(contents=[{
            "source_content_id": "video-1",
            "content_type": "video",
            "title": "Education methods for schools",
            "description": "Learning and teaching",
            "published_at": "2026-08-08T00:00:00Z",
            "view_count": 1000,
        }])
        result = analyze_tags(
            snapshot,
            field_result({"level_1": "Pets & Animals", "level_2": ["Cats"]}),
            field_result("English"),
            AnalysisPolicy.COMPLETE_ESTIMATE,
        )
        distribution = {
            row["tag"]: row["percentage"]
            for row in result.value["top_5_distribution"]
        }
        top_five = result.value["top_5_distribution"][:5]
        self.assertIn("Cats", distribution)
        self.assertGreater(distribution["Education"], distribution["Cats"])
        self.assertEqual(
            [row["tag"] for row in top_five],
            result.value["tags"][:5],
        )
        self.assertEqual(
            [row["percentage"] for row in top_five],
            sorted((row["percentage"] for row in top_five), reverse=True),
        )

    def test_uncategorized_is_not_emitted_as_a_tag(self):
        result = analyze_tags(
            self.snapshot(contents=[{
                "source_content_id": "live-1",
                "content_type": "live",
                "title": "Weekly stream",
                "published_at": "2026-08-08T00:00:00Z",
                "view_count": 1000,
            }]),
            field_result({"level_1": "Uncategorized", "level_2": ["Uncategorized"]}, strength="weak"),
            field_result("English"),
            AnalysisPolicy.COMPLETE_ESTIMATE,
        )
        self.assertNotIn("Uncategorized", result.value["tags"])
        self.assertEqual(len(result.value["tags"]), 10)
        self.assertEqual(len(set(result.value["tags"])), 10)
        self.assertNotIn(
            "Storytelling",
            [row["tag"] for row in result.value["top_5_distribution"][:5]],
        )
        self.assertEqual(
            set(result.metadata["supported_tags"]) & set(result.metadata["fallback_tags"]),
            set(),
        )

    def test_isolated_video_subject_is_not_a_stable_channel_tag(self):
        contents = [
            {
                "source_content_id": f"football-{index}",
                "content_type": "video",
                "title": (
                    "Why the player has a tattoo"
                    if index == 0
                    else f"Football facts and match stories {index}"
                ),
                "published_at": f"2026-08-{8-index:02d}T00:00:00Z",
                "view_count": 1000,
            }
            for index in range(6)
        ]
        result = analyze_tags(
            self.snapshot(contents=contents),
            field_result({"level_1": "Sports & Outdoors", "level_2": ["Ball Sports"]}),
            field_result("English"),
            AnalysisPolicy.COMPLETE_ESTIMATE,
        )
        self.assertNotIn("Tattoo", result.value["tags"])
        self.assertEqual(result.metadata["minimum_stable_content_mentions"], 2)
        self.assertEqual(result.metadata["content_support_counts"].get("Tattoo"), None)

    def test_stable_semantic_topics_rank_ahead_of_format_tags(self):
        snapshot = self.snapshot(
            about="Produtora especializada em produção audiovisual e edição de vídeo.",
            contents=[{
                "source_content_id": f"film-{index}",
                "content_type": "video",
                "title": f"Film production and video editing project {index}",
                "published_at": f"2026-08-{8-index:02d}T00:00:00Z",
                "view_count": 1000,
            } for index in range(5)],
        )
        result = analyze_tags(
            snapshot,
            field_result({"level_1": "Workplace", "level_2": ["Media"]}),
            field_result("Portuguese"),
            AnalysisPolicy.COMPLETE_ESTIMATE,
        )
        top_five = [row["tag"] for row in result.value["top_5_distribution"][:5]]
        self.assertIn("Film Production", top_five)
        self.assertIn("Audiovisual Services", top_five)
        self.assertIn("Video Editing", top_five)
        self.assertLess(
            result.value["tags"].index("Film Production"),
            result.value["tags"].index("Long-form Video"),
        )

    def test_format_and_filler_tags_do_not_occupy_top_five_when_topics_exist(self):
        snapshot = self.snapshot(
            title="GEMEAS VIVI",
            about="Irmãs compartilham vlogs de viagem, rotina e a vida com os cachorros.",
            contents=[
                {
                    "source_content_id": f"vlog-{index}",
                    "content_type": "video" if index % 2 == 0 else "short",
                    "title": (
                        f"Viagem em família e vlog com os cachorros {index}"
                        if index < 4
                        else f"Rotina de cuidados com o cabelo natural {index}"
                    ),
                    "published_at": f"2026-08-0{8-index}T00:00:00Z",
                    "view_count": 1000,
                }
                for index in range(6)
            ],
        )
        result = analyze_tags(
            snapshot,
            field_result({"level_1": "Casual Vlogs", "level_2": ["Lifestyle Vlogs"]}),
            field_result("Portuguese"),
            AnalysisPolicy.COMPLETE_ESTIMATE,
        )
        top_five = [row["tag"] for row in result.value["top_5_distribution"][:5]]
        self.assertNotIn("Long-form Video", top_five)
        self.assertNotIn("Short-form Video", top_five)
        self.assertNotIn("Digital Culture", top_five)
        self.assertTrue(
            {"Travel Vlogs", "Travel", "Dogs", "Natural Hair Care", "Lifestyle Vlogs"}
            & set(top_five)
        )


if __name__ == "__main__":
    unittest.main()
