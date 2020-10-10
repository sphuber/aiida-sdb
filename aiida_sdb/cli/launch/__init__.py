# -*- coding: utf-8 -*-
# pylint: disable=cyclic-import,unused-import,wrong-import-position
"""Module with CLI commands to launch the various workflow steps of the project."""
from .. import cmd_root


@cmd_root.group('launch')
def cmd_launch():
    """Commands to launch the various workflow steps of the project.

    The workflow consistes of the following steps, in this order:

    \b
     1. import
     2. clean
     3. unique
    """


# Import the sub commands to register them with the CLI
from .cif_clean import cif_clean
from .cif_import import cif_import
from .cif_unique import cif_unique
