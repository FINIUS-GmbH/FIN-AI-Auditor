PYTHON ?= python3
WEB_DIR := web

.PHONY: api web worker test

api:
	$(PYTHON) -m fin_ai_auditor.main

worker:
	$(PYTHON) -m fin_ai_auditor.worker.main --once

web:
	cd $(WEB_DIR) && npm run dev

test:
	$(PYTHON) -m pytest
