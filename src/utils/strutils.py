#!/usr/bin/env python3


def utf8len(s):
    """Returns the UTF-8 encoded byte size of a string. `s` is a string parameter."""
    return len(s.encode("utf-8"))
