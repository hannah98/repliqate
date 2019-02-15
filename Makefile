bootstrap:
	pip install -r requirements.txt

lint:
	flake8 repliqate

build:
	python setup.py install

.PHONY: bootstrap lint build
