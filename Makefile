bootstrap:
	pip install -r requirements.txt

lint:
	flake8 repliqate

.PHONY: bootstrap lint
