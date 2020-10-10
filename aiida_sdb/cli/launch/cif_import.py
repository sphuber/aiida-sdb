# -*- coding: utf-8 -*-
# yapf:disable
"""Command to launch the CIF importing step of the project workflow."""
import click

from aiida.cmdline.params import options
from aiida.cmdline.utils import decorators, echo

from . import cmd_launch


@cmd_launch.command('import')
@click.argument('database', type=click.Choice(['cod', 'icsd', 'mpds']), required=True)
@click.option(
    '-m', '--max-number-species', type=click.INT, default=30, show_default=True,
    help='Import only files with at most this number of different species.')
@click.option('-K', '--importer-api-key', type=click.STRING, required=False, help='Optional API key for the database.')
@options.DRY_RUN()
@decorators.with_dbenv()
def cif_import(ctx, database, max_number_species, importer_api_key, dry_run):
    """Import structures from an external database.

    This command will call the `aiida-codtools data cif import` CLI script to perform the actual importing.
    The imported CIF files will be turned into `CifData` nodes and stored in the group `{database}/cif/raw`. The output
    of the script will be piped to a file in a folder that bears the name of the chosen database and the filename is
    created by the current date. This way it is easy to see when this script was ran for the last time. Simply by
    rerunning this script, any new CIF files that have been added to the external database since the last import will be
    simply added to the group.
    """
    import errno
    import os
    import sys

    from datetime import datetime
    from aiida.orm import Group
    from aiida_codtools.cli.misc.cif_import import launch_cif_import

    directory = database
    filepath = '{}.log'.format(os.path.join(database, datetime.utcnow().strftime('%Y%m%d')))

    try:
        os.makedirs(directory)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

    if os.path.isfile(filepath):
        echo.echo_critical('file `{}` already exists, delete it first if you want to continue'.format(filepath))

    group_cif_raw = Group.get(label='{database}/cif/raw'.format(database=database))

    if database == 'cod':
        inputs_database_specific = {}
    elif database == 'icsd':
        inputs_database_specific = {
            'importer_server': 'http://localhost/',
            'importer_db_host': '127.0.0.1',
            'importer_db_name': 'icsd',
            'importer_db_password': 'sql',
        }
    elif database == 'mpds':
        inputs_database_specific = {
            'importer_api_key': importer_api_key
        }

        if max_number_species > 5:
            # Anything above `quinary` will be translated to `multinary`
            max_number_species = 6

    with open(filepath, 'w') as handle:

        sys.stdout = handle

        for number_species in range(1, max_number_species + 1):

            inputs = {
                'group': group_cif_raw,
                'database': database,
                'number_species': number_species,
                'dry_run': dry_run,
            }
            inputs.update(inputs_database_specific)
            ctx.invoke(launch_cif_import, **inputs)
