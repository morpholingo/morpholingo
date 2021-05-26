#!/usr/bin/env python3


def vocabulary_size(tokens):
    """Returns the vocabulary size count defined as the number of alphabetic
    characters as defined by the Python str.isalpha method. This is a
    case-sensitive count. `tokens` is a list of token strings."""
    vocab_list = set(token for token in tokens if token.isalpha())
    return len(vocab_list)
