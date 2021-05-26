#!/usr/bin/env python3

from nltk.tokenize import word_tokenize


def nltk_word_tokenize(s, language="english"):
    """Returns the word and punctuation tokens in a string, `s`.
    `language` is a Punkt trained language tokenizer.
    See https://www.nltk.org/api/nltk.tokenize.html#module-nltk.tokenize.punkt
    """
    return word_tokenize(s, language=language)
