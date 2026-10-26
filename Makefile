.PHONY: install demo test big clean
export PYTHONPATH := src
install:
	pip install -r requirements.txt
demo:
	python -m lakehouse generate --root lake
	python -m lakehouse run --root lake
	python -m lakehouse report --root lake
big:
	python -m lakehouse generate --root demo_big --days 20 --customers 50000 --orders-per-day 25000
	python -m lakehouse run --root demo_big
test:
	python -m pytest -q
clean:
	rm -rf lake demo_big
