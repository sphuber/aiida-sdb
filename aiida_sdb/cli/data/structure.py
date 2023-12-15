# -*- coding: utf-8 -*-
"""Commands to import, create and inspect `StructureData` nodes."""
import click
from aiida.cmdline.params import arguments, options
from aiida.cmdline.utils import decorators, echo

from . import cmd_data

KEY_PARTIAL_OCCUPANCIES = "partial_occupancies"
KEY_SOURCE = "source"
DATABASES = ["cod", "icsd", "mpds"]


@cmd_data.group("structure")
def cmd_structure():
    """Commands to import, create and inspect `StructureData` nodes."""


def get_unmarked_structure_builder():
    """Return builder that queries for structures that do not have the ``KEY_PARTIAL_OCCUPANCIES`` marker."""
    from aiida import orm

    filters = {"extras": {"!has_key": KEY_PARTIAL_OCCUPANCIES}}
    return orm.QueryBuilder().append(orm.StructureData, filters=filters)


def get_cod_hydrogen_structure_ids():
    """Return list of unique COD structures that contain hydrogen."""
    from aiida import orm

    filters = {"extras.chemical_system": {"like": "%-H-%"}}
    builder = orm.QueryBuilder()
    builder.append(orm.Group, filters={"label": "cod/structure/unique"}, tag="group")
    builder.append(orm.Node, with_group="group", filters=filters, project="id")
    return builder.all(flat=True)


@cmd_structure.command("stats")
@arguments.GROUP(required=False)
@click.option(
    "--partial-occupancies/--no-partial-occupancies",
    default=None,
    help="Filter structures for partial occupancies.",
)
@click.option(
    "-s",
    "--skip-check",
    is_flag=True,
    help="Skip the check for unmarked structures. Note if there are "
    "unmarked structures the answer with respect to partial occupancies will be wrong.",
)
@decorators.with_dbenv()
def cmd_stats(group, partial_occupancies, skip_check):
    """Stats about `StructureData` nodes in the database."""
    from aiida import orm

    if not skip_check and get_unmarked_structure_builder().count():
        echo.echo_critical(
            "detected unmarked structures, run `mark-partial-occupancies` first"
        )

    filters = {}

    if partial_occupancies is not None:
        filters[f"extras.{KEY_PARTIAL_OCCUPANCIES}"] = partial_occupancies

    builder = orm.QueryBuilder().append(
        orm.StructureData, filters=filters, tag="structure", project="*"
    )

    if group:
        builder.append(orm.Group, with_node="structure", filters={"id": group.pk})

    echo.echo_info(f"{builder.count()}")


@cmd_structure.command("mark-partial-occupancies")
@decorators.with_dbenv()
def cmd_mark():
    """Mark all `StructureData` nodes with partial occupancies.

    A `StructureData` is said to have partial occupancies if any of its kinds contain more than one symbol (an alloy) or
    the total weight is not unity (vacancies). Any `StructureData` that matches this definition will get an extra set
    with the name `partial_occupancies=True`, otherwise it will be `False`. The reason for setting this extra also on
    structures that do not have partial occupancies, is to make it easy in the future which structures have already been
    considered from marking. When the command is then executed only those structures without mark have to be considered.
    """
    from aiida.manage.manager import get_manager

    builder = get_unmarked_structure_builder()
    unmarked = builder.count()
    echo.echo_info(f"found {unmarked} unmarked structures")

    if not unmarked:
        echo.echo_success("nothing to be done")
        return

    total = 0

    with click.progressbar(
        label="Marking structures", length=unmarked, show_pos=True
    ) as progress:
        with get_manager().get_backend().transaction():
            for [structure] in builder.iterall():
                partial_occupancies = structure.is_alloy or structure.has_vacancies
                structure.set_extra(KEY_PARTIAL_OCCUPANCIES, partial_occupancies)

                if partial_occupancies:
                    total += 1

                progress.update(1)

    echo.echo_success(f"marked {total} structures as containing partial occupancies")


@cmd_structure.command("add-source")
@click.argument("database", type=click.Choice(["cod", "icsd", "mpds"]), required=True)
@decorators.with_dbenv()
def cmd_add_source(database):
    """Copy the source attribute from the original `CifData` to the cleaned and primitivized `StructureData`.

    This is necessary for the uniqueness script that needs to classify the structures based on the external source
    database.
    """
    from aiida import orm

    group_label = f"{database}/structure/primitive"
    filters_group = {"label": group_label}
    filters_structure = {"extras": {"!has_key": KEY_SOURCE}}

    builder = (
        orm.QueryBuilder()
        .append(orm.Group, filters=filters_group, tag="group")
        .append(
            orm.StructureData,
            filters=filters_structure,
            with_group="group",
            project="*",
            tag="structure",
        )
        .append(orm.WorkChainNode, with_outgoing="structure", tag="workchain")
        .append(
            orm.CifData,
            with_outgoing="workchain",
            project=["attributes.source.id", "attributes.source.version"],
        )
    )

    count = builder.count()
    label = f"Adding source info to extras of structure in `{group_label}`"

    if count == 0:
        echo.echo_success(
            f"No structures in group `{group_label}` without source extra"
        )
        return

    with click.progressbar(label=label, length=count, show_pos=True) as progress:
        for structure, source_id, source_version in builder.all():
            source = {
                "database": database,
                "id": source_id,
                "version": source_version,
            }
            structure.set_extra(KEY_SOURCE, source)
            structure.backend_entity.dbmodel.save()
            progress.update(1)

    echo.echo_success(
        f"Added source info to {count} structures in group `{group_label}`"
    )


@cmd_structure.command("uniques")
@arguments.GROUP(required=False)
@click.option(
    "-d",
    "--databases",
    cls=options.MultipleValueOption,
    type=click.Choice(DATABASES),
    help="Filter structures that appear in all of these databases.",
)
@click.option(
    "-e",
    "--elements",
    cls=options.MultipleValueOption,
    type=click.STRING,
    help="Filter structures that appear in all of these databases.",
)
@click.option(
    "-n",
    "--not-elements",
    cls=options.MultipleValueOption,
    type=click.STRING,
    help="Filter structures that appear in all of these databases.",
)
@click.option(
    "--max-atoms",
    type=click.INT,
    default=None,
    show_default=True,
    required=False,
    help="Filter structures with at most this number of atoms.",
)
@click.option(
    "--number-species",
    type=click.INT,
    default=None,
    show_default=True,
    required=False,
    help="Filter structures with at most this number of species.",
)
@click.option(
    "--partial-occupancies/--no-partial-occupancies",
    default=None,
    help="Filter structures for partial occupancies.",
)
@click.option(
    "--no-cod-hydrogen",
    is_flag=True,
    help="Filter structures from the COD containing hydrogen.",
)
@click.option("--count-only", is_flag=True, help="Only count the number of uniques.")
@decorators.with_dbenv()
def cmd_uniques(
    group,
    databases,
    not_elements,
    elements,
    max_atoms,
    number_species,
    partial_occupancies,
    no_cod_hydrogen,
    count_only,
):
    """Print a table of unique formulas with some properties usch as number of atoms and source identifier."""
    from aiida import orm
    from tabulate import tabulate

    filters = {"and": [{"extras": {"!has_key": "incorrect_formula"}}]}

    if not group and not databases:
        raise click.BadParameter(
            "need at least a GROUP or `--databases` to be specified"
        )

    if not group:
        if len(databases) >= 1:
            raise click.BadParameter(
                "can only specify one database when not specifying a GROUP"
            )
        group = orm.load_group(f"{databases[0]}/structure/unique")

    if no_cod_hydrogen:
        filters["and"].append({"id": {"!in": get_cod_hydrogen_structure_ids()}})

    if max_atoms is not None:
        filters["and"].append({"attributes.sites": {"shorter": max_atoms + 1}})

    if number_species is not None:
        filters["and"].append({"attributes.kinds": {"of_length": number_species}})

    if elements:
        filters["and"].append(
            {"extras.chemical_system": {"like": f"%-{'-%-'.join(sorted(elements))}-%"}}
        )

    if not_elements:
        for element in not_elements:
            filters["and"].append(
                {"extras.chemical_system": {"!like": f"%-{element}-%"}}
            )

    if partial_occupancies is not None:
        filters["and"].append(
            {f"extras.{KEY_PARTIAL_OCCUPANCIES}": partial_occupancies}
        )

    if databases:
        for name in DATABASES:
            key = "has_key" if name in databases else "!has_key"
            filters["and"].append({"extras.duplicates": {key: name}})

    builder = (
        orm.QueryBuilder()
        .append(orm.Group, filters={"id": group.id}, tag="group")
        .append(orm.StructureData, with_group="group", filters=filters)
    )

    echo.echo(f"{builder.count()}")

    if count_only:
        return

    rows = []
    for [structure] in builder.iterall():
        rows.append(
            (
                structure.get_formula(),
                len(structure.kinds),
                len(structure.sites),
                structure.uuid,
                structure.get_extra("source")["id"],
            )
        )

    echo.echo(
        tabulate(
            rows,
            headers=["Formula", "# species", "# atoms", "UUID", "Source identifier"],
        )
    )


@cmd_structure.command("export")
@arguments.GROUP()
@click.option(
    "-M",
    "--max-atoms",
    type=click.INT,
    default=None,
    required=False,
    help="Filter structures with at most this number of atoms.",
)
@click.option(
    "-Z",
    "--max-atomic-number",
    type=click.INT,
    default=None,
    required=False,
    help="Filter structures with at most this atomic number.",
)
@click.option(
    "--include-duplicates",
    is_flag=True,
    help="Include also all duplicates of the structures matched for export.",
)
@click.option(
    "--no-cod-hydrogen",
    is_flag=True,
    help="Filter structures from the COD containing hydrogen.",
)
@click.option(
    "--sssp-only",
    is_flag=True,
    help="Filter structures containing elements that are not supported by the SSSP.",
)
@click.option(
    "--filename",
    type=click.Path(),
    default="export.aiida",
    help="The filename of the export archive that will be created.",
)
@decorators.with_dbenv()
def cmd_export(
    group,
    max_atoms,
    max_atomic_number,
    include_duplicates,
    no_cod_hydrogen,
    sssp_only,
    filename,
):
    """Pass."""
    from aiida import orm
    from aiida.common.constants import elements
    from aiida.tools.archive.create import create_archive

    filters_elements = set()
    filters_structures = {"and": []}

    if no_cod_hydrogen:
        filters_structures["and"].append(
            {"id": {"!in": get_cod_hydrogen_structure_ids()}}
        )

    if max_atoms is not None:
        filters_structures["and"].append(
            {"attributes.sites": {"shorter": max_atoms + 1}}
        )

    if max_atomic_number:
        filters_elements = filters_elements.union(
            {e["symbol"] for z, e in elements.items() if z > max_atomic_number}
        )

    if sssp_only:
        # All elements with atomic number of Radon or lower, with the exception of Astatine
        filters_elements = filters_elements.union(
            {e["symbol"] for z, e in elements.items() if z > 86 or z == 85}
        )

    builder = (
        orm.QueryBuilder()
        .append(orm.Group, filters={"id": group.pk}, tag="group")
        .append(orm.StructureData, with_group="group", filters=filters_structures)
    )

    duplicates = []

    if max_atomic_number or sssp_only:
        structures = []
        for (structure,) in builder.iterall():
            if all(
                element not in filters_elements
                for element in structure.get_symbols_set()
            ):
                structures.append(structure)
    else:
        structures = builder.all(flat=True)

    if include_duplicates:
        for structure in structures:
            dupes = []
            structure_duplicates = structure.get_extra("duplicates")
            for uuids in structure_duplicates.values():
                dupes.extend(uuids)
            for duplicate in dupes:
                if duplicate != structure.uuid:
                    duplicates.append(orm.load_node(duplicate))

    create_archive(
        structures + duplicates,
        filename=filename,
        create_backward=False,
        return_backward=False,
    )
