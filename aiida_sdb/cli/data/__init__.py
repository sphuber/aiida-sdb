# -*- coding: utf-8 -*-
# pylint: disable=cyclic-import,unused-import,wrong-import-position
"""Module with CLI commands for various data types."""
from .. import cmd_root


@cmd_root.group('data')
def cmd_data():
    """Commands to import, create and inspect data nodes."""


from aiida.cmdline.params import arguments, options
from aiida.cmdline.utils import decorators, echo


def delete_process(process, dry_run=True, force=False, verbose=False):
    """Delete a process and its children recursively."""
    from aiida import orm
    from aiida.common import exceptions
    from aiida.manage.database.delete.nodes import delete_nodes

    def has_descendants(pk):
        """Check whether a node has descendants."""
        from aiida.orm import Node, QueryBuilder

        builder = QueryBuilder().append(
            Node, filters={'id': pk}, tag='origin').append(
            Node, with_incoming='origin', project='id')

        if builder.first():
            return True
        else:
            return False

    verbosity = 2 if verbose else 0

    # Get child processes and call this function recursively
    builder = orm.QueryBuilder().append(
        orm.Node, filters={'id': process.pk}, tag='origin').append(
        orm.ProcessNode, with_incoming='origin', project=['*'])

    for child, in builder.all():
        delete_process(child, dry_run=dry_run, force=force, verbose=verbose)

    # Get nodes of incoming links
    builder = orm.QueryBuilder().append(
        orm.ProcessNode, filters={'id': process.pk}, tag='parent').append(
        orm.Data, with_outgoing='parent', project=['id'])

    incoming = [entry[0] for entry in builder.all()]

    delete_nodes([process.pk], dry_run=dry_run, force=force, verbosity=verbosity)

    orphans = []
    for pk in incoming:
        try:
            orm.load_node(pk)
        except exceptions.NotExistent:
            continue
        else:
            if not has_descendants(pk):
                orphans.append(pk)

    if dry_run:
        logs = orm.Log.objects.get_logs_for(process)
        echo.echo_info('Would have deleted logs: {}'.format([log.pk for log in logs]))
        echo.echo_info('Would have deleted orphaned inputs: {}'.format([orphan for orphan in orphans]))
    else:
        echo.echo_info('Deleting orphaned inputs: {}'.format(orphans))
        orm.Log.objects.delete_many(filters={'dbnode_id': process.pk})
        delete_nodes(orphans, dry_run=dry_run, force=force, verbosity=verbosity)


@cmd_data.command('delete')
@arguments.PROCESSES()
@options.FORCE()
@options.VERBOSE()
@options.DRY_RUN()
@decorators.with_dbenv()
def cmd_delete(processes, force, verbose, dry_run):
    """Pass"""
    from aiida.manage.database.delete.nodes import delete_nodes
    verbosity = 2 if verbose else 0
    delete_nodes([process.pk for process in processes], dry_run=dry_run, force=force, verbosity=verbosity)



# Import the sub commands to register them with the CLI
from .structure import cmd_structure
