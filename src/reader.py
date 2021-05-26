#!/usr/bin/env python3

import argparse
import json
import sys

from meta import __version__
from parsers.html import parse_html_p_content
from preprocessing.cleaners import clean_wikipedia


def main(argv):
    parser = argparse.ArgumentParser(description="JSON content text reader")
    parser.add_argument(
        "--version", action="version", version=f"reader.py v{__version__}"
    )
    parser.add_argument("FILEPATH", nargs="+", help="JSON file path(s)")
    args = parser.parse_args(argv)

    for json_path in args.FILEPATH:
        with open(json_path, "r") as f:
            json_text = f.read()
            json_obj = json.loads(json_text)
            html_text = json_obj["parse"]["text"]
            content = parse_html_p_content(html_text)
            if content:
                cleaned_content = clean_wikipedia(content)
                print(cleaned_content)
            else:
                sys.stderr.write(f"[ERROR] {json_path}: No content available")
                sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
