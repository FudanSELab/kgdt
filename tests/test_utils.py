#!/usr/bin/env python

"""Tests for `kgdt` package."""

from kgdt.utils import SaveLoad


class ABC(SaveLoad):
    def __init__(self, id):
        self.id = id

    def print(self):
        print("load " + str(self.id))


def test_save_load():
    """Test the CLI."""
    ABC(3).save("abc.abc")
    abc = ABC.load("abc.abc")
    assert abc.id == 3
    abc.print()
