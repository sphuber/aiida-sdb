# -*- coding: utf-8 -*-
# pylint: disable=wrong-import-position
"""Module for the command line interface."""
import click
from aiida.cmdline.groups import VerdiCommandGroup
from aiida.cmdline.params import options, types


@click.group(
    "aiida-sdb",
    cls=VerdiCommandGroup,
    context_settings={"help_option_names": ["-h", "--help"]},
)
@options.PROFILE(type=types.ProfileParamType(load_profile=True))
def cmd_root(profile):  # pylint: disable=unused-argument
    """CLI for the SDB project."""
