"""python -m beacon_sdk.dev: values from the command line and the checks Beacon would make."""

import unittest

from beacon_sdk.dev import coerce, problems

SCHEMA = {"additionalProperties": False, "properties": {
    "n": {"type": "integer", "minimum": 1}, "on": {"type": "boolean"}, "mode": {"type": "string", "enum": ["a", "b"]},
    "res": {"type": "array", "items": {"type": "integer"}}}}


class DevTest(unittest.TestCase):
    def test_coerce(self):
        p = SCHEMA["properties"]
        self.assertEqual(coerce("30", p["n"]), 30)
        self.assertIs(coerce("false", p["on"]), False)
        self.assertEqual(coerce("1080, 720", p["res"]), [1080, 720])
        self.assertEqual(coerce("div img", {}), "div img")
        with self.assertRaises(ValueError):
            coerce("maybe", p["on"])

    def test_problems(self):
        self.assertEqual(problems({"n": 2, "on": True, "mode": "a"}, SCHEMA, "config"), [])
        bad = problems({"n": 0, "mode": "c", "on": "yes", "typo": 1}, SCHEMA, "config")
        self.assertEqual(len(bad), 4, bad)
        self.assertTrue(any("typo" in b for b in bad))


if __name__ == "__main__":
    unittest.main()
