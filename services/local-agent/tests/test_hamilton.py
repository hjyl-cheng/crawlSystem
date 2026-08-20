import unittest

from qy_channel_profile.errors import ContractError
from qy_channel_profile.hamilton import apportion


class HamiltonTest(unittest.TestCase):
    def test_exact_total_and_stable_tie_break(self):
        result = apportion({"a": 1, "b": 1, "c": 1}, total=100, order=["a", "b", "c"])
        self.assertEqual(result, {"a": 34, "b": 33, "c": 33})
        self.assertEqual(sum(result.values()), 100)

    def test_rejects_all_zero_input(self):
        with self.assertRaises(ContractError):
            apportion({"a": 0, "b": 0})


if __name__ == "__main__":
    unittest.main()

