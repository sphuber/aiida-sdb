# -*- coding: utf-8 -*-
# pylint: disable=cyclic-import,unused-import,wrong-import-position
"""Module with CLI commands to analyse the contents of the database."""
from .. import cmd_root


@cmd_root.group("analyse")
def cmd_analyse():
    """Commands to analyse the contents of the database."""
