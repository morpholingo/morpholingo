all:

format: format-import format-black

format-black:
	black src

format-import:
	isort src



.PHONY: all