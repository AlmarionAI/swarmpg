import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("outbox_recovery", Path(__file__).with_name("outbox-recovery.py"))
recovery = importlib.util.module_from_spec(spec)
spec.loader.exec_module(recovery)


class ReceiverEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.intent = {"id": "message", "org_id": "organization", "task_id": "task", "key": "effect",
                       "destination": "tool", "payload": {"value": 42}, "idempotency_key": "stable-key"}

    def row(self, body=None):
        encoded = json.dumps(self.intent if body is None else body)
        return (self.intent["id"], self.intent["idempotency_key"], hashlib.sha256(encoded.encode()).hexdigest(), encoded)

    def test_matches_original_intent(self):
        recovery.validate_receiver_effects([self.intent], [self.row()])

    def test_rejects_consistently_corrupted_payload(self):
        changed = copy.deepcopy(self.intent)
        changed["payload"]["value"] = 99
        with self.assertRaisesRegex(AssertionError, "differs from committed intent"):
            recovery.validate_receiver_effects([self.intent], [self.row(changed)])

    def test_rejects_changed_attribution_or_destination(self):
        for field in ("id", "org_id", "task_id", "key", "destination", "idempotency_key"):
            with self.subTest(field=field):
                changed = {**self.intent, field: "wrong"}
                with self.assertRaises(AssertionError):
                    recovery.validate_receiver_effects([self.intent], [self.row(changed)])

    def test_rejects_number_changed_to_boolean(self):
        for number, boolean in ((1, True), (0, False)):
            with self.subTest(number=number):
                self.intent["payload"]["value"] = number
                changed = copy.deepcopy(self.intent)
                changed["payload"]["value"] = boolean
                with self.assertRaisesRegex(AssertionError, "differs from committed intent"):
                    recovery.validate_receiver_effects([self.intent], [self.row(changed)])

    def test_rejects_missing_duplicate_and_forged_digest(self):
        for rows in ([], [self.row(), self.row()], [(*self.row()[:2], "bad-digest", self.row()[3])]):
            with self.subTest(rows=rows), self.assertRaises(AssertionError):
                recovery.validate_receiver_effects([self.intent], rows)


if __name__ == "__main__":
    unittest.main()
