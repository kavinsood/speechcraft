.PHONY: help setup setup-backend setup-frontend bootstrap dev-backend dev-api dev-worker dev-frontend check check-backend check-frontend backend-docs smoke-backend

UV_CACHE_DIR ?= /tmp/uv-cache
BACKEND_HOST ?= 127.0.0.1
BACKEND_PORT ?= 8010
FRONTEND_API_BASE_URL ?= http://$(BACKEND_HOST):$(BACKEND_PORT)
SMOKE_BACKEND_BASE_URL ?= http://127.0.0.1:8010
SMOKE_BACKEND_PROJECT_ID ?= phase1-demo
LOCAL_MFA_BIN := $(HOME)/.conda/envs/speechcraft-mfa/bin/mfa
ifeq ($(wildcard $(LOCAL_MFA_BIN)),$(LOCAL_MFA_BIN))
SPEECHCRAFT_MFA_BIN ?= $(LOCAL_MFA_BIN)
endif
SPEECHCRAFT_MFA_DICTIONARY ?= english_us_mfa
SPEECHCRAFT_MFA_ACOUSTIC_MODEL ?= english_mfa
ROOT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
BACKEND_DIR := $(ROOT_DIR)/backend
FRONTEND_DIR := $(ROOT_DIR)/frontend
WORKER_ROOT := $(ROOT_DIR)/workers/dataset

export SPEECHCRAFT_MFA_BIN
export SPEECHCRAFT_MFA_DICTIONARY
export SPEECHCRAFT_MFA_ACOUSTIC_MODEL

help:
	@printf "\nSpeechcraft commands:\n"
	@printf "  make setup           Install backend and frontend dependencies\n"
	@printf "  make bootstrap       Alias for setup\n"
	@printf "  make dev-backend     Run the FastAPI API and ProcessingJob worker\n"
	@printf "  make dev-api         Run only the FastAPI API with reload\n"
	@printf "  make dev-worker      Run only the ProcessingJob worker\n"
	@printf "  make dev-frontend    Run the Next.js frontend dev server (http://127.0.0.1:3002)\n"
	@printf "  make check           Run backend and frontend verification\n"
	@printf "  make check-backend   Compile-check backend Python code\n"
	@printf "  make check-frontend  Build the frontend production bundle\n"
	@printf "  make smoke-backend   Run non-destructive smoke checks against a running backend\n"
	@printf "  make backend-docs    Run the backend and use /docs at http://127.0.0.1:8010/docs\n"
	@printf "\n"

setup: setup-backend setup-frontend

bootstrap: setup

setup-backend:
	cd $(BACKEND_DIR) && UV_CACHE_DIR=$(UV_CACHE_DIR) uv sync

setup-frontend:
	cd $(FRONTEND_DIR) && bun install

dev-backend:
	cd $(BACKEND_DIR) && UV_CACHE_DIR=$(UV_CACHE_DIR) sh -c 'set -e; mkdir -p logs; : > logs/dev-backend.log; (uv run uvicorn app.main:app --reload --host $(BACKEND_HOST) --port $(BACKEND_PORT) 2>&1 | tee -a logs/dev-backend.log) & api_pid=$$!; (uv run python -m app.worker 2>&1 | tee -a logs/dev-backend.log) & worker_pid=$$!; trap "kill $$api_pid $$worker_pid 2>/dev/null || true" INT TERM EXIT; wait'

dev-api:
	cd $(BACKEND_DIR) && UV_CACHE_DIR=$(UV_CACHE_DIR) uv run uvicorn app.main:app --reload --host $(BACKEND_HOST) --port $(BACKEND_PORT)

dev-worker:
	cd $(BACKEND_DIR) && UV_CACHE_DIR=$(UV_CACHE_DIR) uv run python -m app.worker

backend-docs: dev-backend

dev-frontend:
	cd $(FRONTEND_DIR) && SPEECHCRAFT_BACKEND_URL=$(FRONTEND_API_BASE_URL) bun run dev

check: check-backend check-frontend

check-backend:
	cd $(ROOT_DIR) && python3 -m compileall backend/app
	cd $(BACKEND_DIR) && ./.venv/bin/python -m unittest discover -s tests -p 'test_*.py'
	cd $(WORKER_ROOT) && PYTHONPATH=. $(BACKEND_DIR)/.venv/bin/python tests/test_clip_lab_coordination.py

check-frontend:
	cd $(FRONTEND_DIR) && bun run build

smoke-backend:
	cd $(BACKEND_DIR) && UV_CACHE_DIR=$(UV_CACHE_DIR) uv run python scripts/smoke_backend.py --base-url $(SMOKE_BACKEND_BASE_URL) --project-id $(SMOKE_BACKEND_PROJECT_ID)
