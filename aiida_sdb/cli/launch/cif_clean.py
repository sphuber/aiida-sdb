# -*- coding: utf-8 -*-
"""Command to launch the CIF cleaning step of the project workflow."""
import click
from aiida.cmdline.params import options, types
from aiida.cmdline.utils import decorators, echo

from . import cmd_launch


@cmd_launch.command("clean")
@click.argument("database", type=click.Choice(["cod", "icsd", "mpds"]), required=True)
@click.option(
    "-F",
    "--cif-filter",
    required=True,
    type=types.CodeParamType(entry_point="codtools.cif_filter"),
    help="Code that references the codtools cif_filter script.",
)
@click.option(
    "-S",
    "--cif-select",
    required=True,
    type=types.CodeParamType(entry_point="codtools.cif_select"),
    help="Code that references the codtools cif_select script.",
)
@click.option(
    "--concurrent",
    type=click.INT,
    default=500,
    help="Number of maximum concurrent work chains to submit.",
)
@click.option(
    "--interval",
    type=click.INT,
    default=30,
    help="Number of seconds to sleep after a submit round.",
)
@options.DRY_RUN()
@click.pass_context
@decorators.with_dbenv()
def cif_clean(ctx, database, cif_filter, cif_select, concurrent, interval, dry_run):
    """Clean the CIF files imported from an external database.

    Run the `aiida-codtools workflow launch cif-clean` CLI script to clean the imported CIFs of the given database.
    """
    from datetime import datetime
    from pprint import pprint
    from time import sleep

    from aiida import orm
    from aiida_codtools.cli.workflows.cif_clean import launch_cif_clean

    now = datetime.utcnow().isoformat

    group_cif_raw = orm.Group.get(label=f"{database}/cif/raw")
    group_cif_clean = orm.Group.get(label=f"{database}/cif/clean")
    group_structure = orm.Group.get(label=f"{database}/structure/primitive")
    group_workchain = orm.Group.get(label=f"{database}/workchain/clean")

    while True:
        filters = {
            "attributes.process_state": {"or": [{"==": "excepted"}, {"==": "killed"}]}
        }
        builder = orm.QueryBuilder().append(orm.ProcessNode, filters=filters)
        if builder.count() > 0:
            echo.echo_critical(
                f"found {builder.count()} excepted or killed processes, exiting"
            )

        filters = {
            "attributes.process_state": {
                "or": [{"==": "waiting"}, {"==": "running"}, {"==": "created"}]
            }
        }
        builder = orm.QueryBuilder().append(orm.WorkChainNode, filters=filters)
        current = builder.count()
        max_entries = concurrent - current

        if current < concurrent:
            echo.echo(
                f"{now()} | currently {current} running workchains, submitting {max_entries} more"
            )

            inputs = {
                "cif_filter": cif_filter,
                "cif_select": cif_select,
                "group_cif_raw": group_cif_raw,
                "group_cif_clean": group_cif_clean,
                "group_structure": group_structure,
                "group_workchain": group_workchain,
                "max_entries": max_entries,
                "skip_check": False,
                "parse_engine": "ase",
                "daemon": True,
            }
            if dry_run:
                echo.echo(pprint(inputs))
                return

            ctx.invoke(launch_cif_clean, **inputs)
        else:
            echo.echo(
                f"{now()} | currently {current} running workchains, nothing to submit"
            )

        echo.echo(f"{now()} | sleeping {interval} seconds")
        sleep(interval)
