from __future__ import annotations

from app.main import get_allowed_origins

from tests.http_support import LiveServerTestCase


class ApiRequestTests(LiveServerTestCase):

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

    def test_healthz_route_returns_ok_json(self) -> None:
        response = self.client.get("/healthz")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})
        self.assertEqual(response.headers["content-type"], "application/json")

    def test_project_routes_return_seeded_demo_project(self) -> None:
        projects_response = self.client.get("/api/projects")
        self.assertEqual(projects_response.status_code, 200)
        projects = projects_response.json()
        self.assertEqual(len(projects), 1)
        self.assertEqual(projects[0]["id"], "phase1-demo")

        slices_response = self.client.get("/api/projects/phase1-demo/slices")
        self.assertEqual(slices_response.status_code, 200)
        slices = slices_response.json()
        self.assertEqual(len(slices), 1)
        self.assertEqual(slices[0]["id"], "clip-001")

        preview_response = self.client.get("/api/projects/phase1-demo/export-preview")
        self.assertEqual(preview_response.status_code, 200)
        preview = preview_response.json()
        self.assertEqual(preview["project_id"], "phase1-demo")
        self.assertEqual(preview["accepted_slice_count"], 0)

    def test_missing_project_route_returns_404(self) -> None:
        response = self.client.get("/api/projects/missing-project/export-preview")

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["detail"], "Project not found")

    def test_cors_preflight_uses_explicit_origin_for_allowed_frontend(self) -> None:
        response = self.client.options(
            "/api/projects",
            headers={
                "Origin": "http://127.0.0.1:5173",
                "Access-Control-Request-Method": "GET",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("access-control-allow-origin"), "http://127.0.0.1:5173")
        self.assertEqual(response.headers.get("access-control-allow-credentials"), "true")
        self.assertNotEqual(response.headers.get("access-control-allow-origin"), "*")

    def test_cors_preflight_rejects_unknown_origin(self) -> None:
        response = self.client.options(
            "/api/projects",
            headers={
                "Origin": "https://evil.example",
                "Access-Control-Request-Method": "GET",
            },
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("Disallowed CORS origin", response.text)
        self.assertNotIn("access-control-allow-origin", response.headers)
