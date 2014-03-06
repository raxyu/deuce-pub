from unittest import TestCase
from deuce.util import FileCat

from deuce.controllers.validation import ValidationFailed
from deuce.controllers import validation as v
import os

# TODO: We probably want to move this to a
# test helpers library


class TestValidation(TestCase):

    def test_vault_id(self):

        positive_cases = [
            'a',
            '0',
            '__vault_id____',
            '-_-_-_-_-_-_-_-',
            'snake_case_is_ok',
            'So-are-hyphonated-names',
            'a' * v.VAULT_ID_MAX_LEN
        ]

        for name in positive_cases:
            v.validate_vault_id(name)

        negative_cases = [
            '',  # empty case should raise
            '.', '!', '@', '#', '$', '%',
            '^', '&', '*', '[', ']', '/',
            '@#$@#$@#^@%$@#@#@#$@!!!@$@$@',
            '\\', 'a' * (v.VAULT_ID_MAX_LEN + 1)
        ]

        for name in negative_cases:
            with self.assertRaises(ValidationFailed):
                v.validate_vault_id(name)

    def test_block_id(self):

        positive_cases = [
            'da39a3ee5e6b4b0d3255bfef95601890afd80709',
            'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            'ffffffffffffffffffffffffffffffffffffffff',
            'a' * 40,
        ]

        for blockid in positive_cases:
            v.validate_block_id(blockid)

        negative_cases = [
            '',
            '.',
            'a', '0', 'f', 'F', 'z', '#', '$', '?',
            'a39a3ee5e6b4b0d3255bfef95601890afd80709',  # one char short
            'da39a3ee5e6b4b0d3255bfef95601890afd80709a',  # one char long
            'DA39A3EE5E6B4B0D3255BFEF95601890AFD80709',
            'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF',
            'AaaAaaAaaaaAaAaaaAaaaaaaaAAAAaaaaAaaaaaa' * 2,
            'AaaAaaAaaaaAaAaaaAaaaaaaaAAAAaaaaAaaaaaa' * 3,
            'AaaAaaAaaaaAaAaaaAaaaaaaaAAAAaaaaAaaaaaa' * 4
        ]

        for blockid in negative_cases:
            with self.assertRaises(ValidationFailed):
                v.validate_block_id(blockid)

    def test_file_id(self):

        import uuid

        # Let's try try to append some UUIds and check for faileus
        positive_cases = {str(uuid.uuid4()) for _ in range(0, 1000)}

        for fileid in positive_cases:
            v.validate_file_id(fileid)

        negative_cases = [
            '',
            'e7bf692b-ec7b-40ad-b0d1-45ce6798fb6z',  # note trailing z
            str(uuid.uuid4()).upper()  # Force case sensitivity
        ]

        for fileid in negative_cases:
            with self.assertRaises(ValidationFailed):
                v.validate_file_id(fileid)
