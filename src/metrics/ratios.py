#!/usr/bin/env python3


def lexical_diversity(tokens):
    """Returns a *case-sensitive* lexical diversity measure.  We want to keep case forms
    of the same word as these are considered different tokens in this corpus. `tokens`
    is a list of token strings."""
    return len(set(tokens)) / len(tokens)
