import unittest
from pathlib import Path

from qy_channel_profile.language_id import FastTextLanguageIdentifier
from qy_channel_profile.text_features import TextUnit


class FastTextLanguageIdentifierTest(unittest.TestCase):
    def test_local_fasttext_model_detects_spanish_without_network(self):
        path = Path("artifacts/models/external/lid.176.ftz")
        if not path.is_file():
            self.skipTest("local FastText artifact is not installed")
        detector = FastTextLanguageIdentifier(path)
        evidence = detector.detect([
            TextUnit(
                "channel_about",
                "Hola, bienvenidos a nuestro canal. Hoy tenemos un nuevo video para ustedes.",
                4.0,
            )
        ])
        self.assertEqual(max(evidence.probabilities, key=evidence.probabilities.get), "Spanish")
        self.assertGreater(evidence.probabilities["Spanish"], 0.8)

    def test_repeated_description_family_cannot_overwhelm_channel_about(self):
        path = Path("artifacts/models/external/lid.176.ftz")
        if not path.is_file():
            self.skipTest("local FastText artifact is not installed")
        detector = FastTextLanguageIdentifier(path)
        units = [TextUnit(
            "channel_about",
            "Hola, soy una creadora mexicana y este canal siempre publica videos en español.",
            4.0,
        )]
        units.extend(
            TextUnit(
                "content_description",
                f"Welcome to this English video number {index} with tutorials and reviews for everyone.",
                1.0,
            )
            for index in range(40)
        )
        evidence = detector.detect(units)
        self.assertEqual(max(evidence.probabilities, key=evidence.probabilities.get), "Spanish")


if __name__ == "__main__":
    unittest.main()
