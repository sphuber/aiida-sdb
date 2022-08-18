# -*- coding: utf-8 -*-
"""Commands to analyse the results of the ``CifCleanWorkChains``."""
import collections

from aiida.cmdline.utils import echo
import click
import tabulate

from . import cmd_analyse

DATABASES = ['cod', 'icsd', 'mpds']


@cmd_analyse.command('cif-clean')
@click.option(
    '-d',
    '--database',
    type=click.Choice(DATABASES),
    help='Perform the analysis only for this database instead of for all.'
)
@click.option(
    '-f',
    '--format',
    'fmt',
    type=click.Choice(['plain', 'latex']),
    default='plain',
    help='Format to use for printing the final table.'
)
def cmd_cif_clean(database, fmt):
    """Print table overview of the exit statuses of completed ``CifCleanWorkChains`` for the various databases."""
    from aiida import orm
    from aiida.plugins import WorkflowFactory

    if database is not None:
        databases = [database]
    else:
        databases = DATABASES

    counters = []

    for db in databases:
        group_workchain_clean = orm.load_group(f'{db}/workchain/clean')

        builder = orm.QueryBuilder()
        builder.append(orm.Group, filters={'label': group_workchain_clean.label}, tag='group')
        builder.append(orm.WorkChainNode, project=['attributes.exit_status'], with_group='group')

        counter = collections.Counter(builder.all(flat=True))
        counters.append(counter)

    table = []
    headers = ['Exit code', 'Description']
    headers.extend(databases)

    exit_codes = sorted(set(exit_code for counter in counters for exit_code in counter.keys()))

    for exit_code in exit_codes:

        if exit_code:
            message = WorkflowFactory('codtools.cif_clean').exit_codes(exit_code).message
            description = message[0].upper() + message[1:]
        else:
            description = 'Success'

        row = [exit_code, description]
        row.extend([counter.get(exit_code, 0) for counter in counters])
        table.append(row)

    echo.echo(tabulate.tabulate(table, headers=headers, tablefmt=fmt))


@cmd_analyse.command('cif-clean-manual')
@click.option(
    '-f',
    '--format',
    'fmt',
    type=click.Choice(['plain', 'latex']),
    default='plain',
    help='Format to use for printing the final table.'
)
def cmd_cif_clean_manual(fmt):
    """Print table with successfully completed workchains that had invalid ``CifData`` as input that went overlooked.

    After the work chains had finished it was discovered that many of the ``CifData`` that were input to a work chain
    that finished successfully were actually inconsistent. A separate analysis compared the input ``CifData`` with the
    parsed ``StructureData`` for their structural formula and found various problems. The affected ``CifData`` nodes,
    both source and cleaned version, as well as the parsed ``StructureData`` were tagged with the ``incorrect_formula``
    extra, that takes the value ``missing_hydrogen``, ``missing_lithium``, ``missing_other`` and ``different_comp``.

    This command will print the numbers of these that are found in each database.
    """
    from aiida import orm

    counters = []

    for db in DATABASES:

        filters = {'extras': {'has_key': 'incorrect_formula'}}

        builder = orm.QueryBuilder()
        builder.append(orm.Group, filters={'label': f'{db}/workchain/clean'}, tag='group')
        builder.append(orm.WorkChainNode, with_group='group', tag='wc', filters={'attributes.exit_status': {'==': 0}})
        builder.append(orm.CifData, with_incoming='wc', filters=filters, project='extras.incorrect_formula')

        counter = collections.Counter(builder.all(flat=True))
        counters.append(counter)

    table = []
    headers = ['Error']
    headers.extend(DATABASES)

    keys = sorted(set(key for counter in counters for key in counter.keys()))

    for key in keys:

        row = [key]
        row.extend([counter.get(key) for counter in counters])
        table.append(row)

    echo.echo(tabulate.tabulate(table, headers=headers, tablefmt=fmt))
