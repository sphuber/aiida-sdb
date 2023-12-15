# -*- coding: utf-8 -*-
# pylint: disable=cyclic-import,unused-import,wrong-import-position
"""Module with CLI commands to launch the various workflow steps of the project."""
from aiida.cmdline.groups import VerdiCommandGroup

from .. import cmd_root


@cmd_root.group(
    "launch",
    cls=VerdiCommandGroup,
    context_settings={"help_option_names": ["-h", "--help"]},
)
def cmd_launch():
    """Commands to launch the various workflow steps of the project.

    The workflow consistes of the following steps, in this order:

    \b
     1. import
     2. clean
     3. unique
    """


# Import the sub commands to register them with the CLI
