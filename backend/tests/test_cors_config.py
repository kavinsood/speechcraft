from __future__ import annotations

from unittest import TestCase

from app.main import get_allowed_origins


class CorsConfigTests(TestCase):
    def test_get_allowed_origins_defaults_to_local_frontend_hosts(self) -> None:
        self.assertEqual(
            get_allowed_origins(""),
            [
                "http://127.0.0.1:4173",
                "http://127.0.0.1:5173",
                "http://localhost:4173",
                "http://localhost:5173",
            ],
        )

    def test_get_allowed_origins_normalizes_and_deduplicates_env_values(self) -> None:
        self.assertEqual(
            get_allowed_origins(
                " http://localhost:5173/,http://127.0.0.1:5173,http://localhost:5173 "
            ),
            ["http://localhost:5173", "http://127.0.0.1:5173"],
        )

    def test_get_allowed_origins_rejects_wildcard_with_credentials(self) -> None:
        with self.assertRaisesRegex(ValueError, "cannot include '\\*'"):
            get_allowed_origins("*")
