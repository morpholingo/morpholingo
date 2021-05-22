all:

format: format-import format-black

format-black:
	black src

format-import:
	isort src


install-ipykernel:
	python -m ipykernel install --name=morpholingo


.PHONY: all