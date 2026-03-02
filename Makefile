.PHONY: help setup setup-backend setup-frontend bootstrap dev-backend dev-frontend check check-backend check-frontend backend-docs

UV_CACHE_DIR ?= /tmp/uv-cache
ROOT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
BACKEND_DIR := $(ROOT_DIR)/backend
FRONTEND_DIR := $(ROOT_DIR)/frontend

help:
	@printf "\nSpeechcraft commands:\n"
	@printf "  make setup           Install backend and frontend dependencies\n"
	@printf "  make bootstrap       Alias for setup\n"
	@printf "  make dev-backend     Run the FastAPI backend with reload\n"
	@printf "  make dev-frontend    Run the Vite frontend dev server\n"
	@printf "  make check           Run backend and frontend verification\n"
	@printf "  make check-backend   Compile-check backend Python code\n"
	@printf "  make check-frontend  Build the frontend production bundle\n"
	@printf "  make backend-docs    Run the backend and use /docs at http://127.0.0.1:8000/docs\n"
	@printf "\n"

setup: setup-backend setup-frontend

bootstrap: setup

setup-backend:
	cd $(BACKEND_DIR) && UV_CACHE_DIR=$(UV_CACHE_DIR) uv sync

setup-frontend:
	cd $(FRONTEND_DIR) && npm install

dev-backend:
	cd $(BACKEND_DIR) && UV_CACHE_DIR=$(UV_CACHE_DIR) uv run uvicorn app.main:app --reload

backend-docs: dev-backend

dev-frontend:
	cd $(FRONTEND_DIR) && npm run dev

check: check-backend check-frontend

check-backend:
	cd $(ROOT_DIR) && python3 -m compileall backend/app

check-frontend:
	cd $(FRONTEND_DIR) && npm run build
