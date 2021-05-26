#!/usr/bin/env python3

from bs4 import BeautifulSoup


def parse_html_p_content(html_text):
    """Parses HTML <p> tag text content and returns as
    a newline-delimited concatenated string.  Parsed
    with BeautifulSoup 4.  Returns either a string or
    None (no content)"""
    soup = BeautifulSoup(html_text, "lxml")
    contents = soup.find_all("p")
    if len(contents) > 0:
        return "\n".join([p.text for p in contents])
    else:
        return None
