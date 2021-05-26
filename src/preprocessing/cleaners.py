#!/usr/bin/env python3
import re

WIKI_REF_RE = re.compile(r"\[\d{1,3}\]")


def clean_wikipedia(text):
    # remove "[edit]" strings that are used for content editing
    cleaned_content = text.replace("[edit]", "")
    # remove "[\d]" style reference strings
    cleaned_content = re.sub(WIKI_REF_RE, "", cleaned_content)
    # remove "[citation neeeded]" editorial strings
    cleaned_content = cleaned_content.replace("[citation needed]", "")

    return cleaned_content
