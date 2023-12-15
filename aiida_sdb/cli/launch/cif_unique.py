# -*- coding: utf-8 -*-
"""Command to launch the CIF uniqueness analysis step of the project workflow."""
import click
from aiida.cmdline.params import arguments
from aiida.cmdline.utils import decorators, echo

from . import cmd_launch


@cmd_launch.command("unique")
@arguments.GROUP("group_candidate", required=True)
@arguments.GROUP("group_reference", required=True)
@click.option(
    "--contains",
    default=None,
    help="Filter structures for a specific chemical formula.",
)
@click.option(
    "--max-size", default=None, help="Filter structures for a maximum number of atoms."
)
@click.option(
    "--partial-occupancies/--no-partial-occupancies",
    default=None,
    help="Filter structures for partial occupancies.",
)
@click.option(
    "--add/--no-add",
    is_flag=True,
    default=False,
    help="Toggle whether to add the new unique prototypes to the reference group.",
)
@decorators.with_dbenv()
def cif_unique(
    group_candidate, group_reference, contains, max_size, partial_occupancies, add
):
    """Perform a uniqueness analysis between groups of structures.

    The structures of GROUP_CANDIDATE are compared against those of GROUP_REFERENCE.
    """
    import collections

    from aiida import orm
    from aiida.manage.manager import get_manager
    from numpy import eye, where
    from pymatgen.analysis.structure_matcher import StructureMatcher
    from scipy.sparse.csgraph import connected_components

    ltol = 0.2
    stol = 0.3
    angle_tol = 5
    scale = True
    primitive_cell = False
    attempt_supercell = False
    max_size = int(max_size) if max_size is not None else None

    mapping = collections.defaultdict(list)
    ordered = collections.OrderedDict()
    unique = collections.defaultdict(dict)
    unique_references = []
    reference_uuids = []
    new_structures = []

    filters = {"and": []}

    if partial_occupancies is not None:
        filters["and"].append({"extras.partial_occupancies": partial_occupancies})

    if contains is not None:
        filters["and"].append({"extras.formula_hill": {"like": rf"%{contains}%"}})

    if max_size is not None:
        filters["and"].append({"extras.number_of_sites": {"<=": max_size}})

    builder_candidate = (
        orm.QueryBuilder()
        .append(orm.Group, filters={"id": group_candidate.id}, tag="group")
        .append(orm.StructureData, with_group="group", filters=filters)
    )

    count = builder_candidate.count()
    label = f"Mapping {count} structures of {group_candidate.label}"

    # Map all structures that are in the candidate group on reduced chemical formula
    with click.progressbar(label=label, length=count, show_pos=True) as progress:
        for [node] in builder_candidate.iterall():
            progress.update(1)
            formula = node.get_formula(mode="hill_compact")
            mapping[formula].append([len(node.sites), node.uuid, node])
            new_structures.append(node)

    builder_reference = (
        orm.QueryBuilder()
        .append(orm.Group, filters={"id": group_reference.id}, tag="group")
        .append(orm.StructureData, with_group="group", filters=filters)
    )

    count = builder_reference.count()
    label = f"Mapping structures of {group_reference.label}"
    candidate_formulas = list(mapping.keys())

    # Map all structures that are in the unique group on reduced chemical formula
    with click.progressbar(label=label, length=count, show_pos=True) as progress:
        for [node] in builder_reference.iterall():
            progress.update(1)
            formula = node.get_formula(mode="hill_compact")
            if formula not in candidate_formulas:
                continue
            mapping[formula].append([len(node.sites), node.uuid, node])
            reference_uuids.append(node.uuid)

    # Create a set to make `in` conditional operation faster
    reference_uuids = set(reference_uuids)

    # Order the mapping alphabetically by chemical formula and within each formula entry by number of atoms and UUID
    for formula, entries in sorted(mapping.items(), key=lambda x: x[0]):
        ordered[formula] = [
            entry[2] for entry in sorted(entries, key=lambda e: (e[0], e[1]))
        ]

    def structure_similarity(matcher, structure_i, structure_j):
        """Return whether structure_i and structure_j are similar according to the given matcher.

        :param matcher: instance of pymatgen.StructureMatcher
        :param structure_i: instance of StructureData
        :param structure_j: instance of StructureData
        :return: integer, 1 if the matcher deems the structures equal, 0 otherwise
        """
        try:
            return int(
                matcher.fit(
                    structure_i.get_pymatgen_structure(),
                    structure_j.get_pymatgen_structure(),
                )
            )
        except TypeError:
            echo.echo_info(
                f"could not match the structures {structure_i.uuid} and {structure_j.uuid}"
            )
            return 0

    matcher = StructureMatcher(
        ltol=ltol,
        stol=stol,
        angle_tol=angle_tol,
        scale=scale,
        primitive_cell=primitive_cell,
        attempt_supercell=attempt_supercell,
    )

    label = "Uniqueness analysis for each compound"
    with click.progressbar(label=label, length=len(ordered), show_pos=True) as progress:
        for compound, structures in ordered.items():
            nstructures = len(structures)
            adjacent_matrix = eye(nstructures, dtype=int)

            for i in range(nstructures):
                for j in range(i + 1, nstructures):
                    adjacent_matrix[i, j] = structure_similarity(
                        matcher, structures[i], structures[j]
                    )
                    adjacent_matrix[j, i] = adjacent_matrix[i, j]

            _, connection = connected_components(adjacent_matrix, directed=False)
            prototype_indices = [
                where(connection == e)[0].tolist() for e in set(connection)
            ]

            for prototype in prototype_indices:
                prototype_structures = [structures[index] for index in prototype]

                # See if any of the prototype structures are already in the set of uuids from the reference group
                for prototype_structure in prototype_structures:
                    if prototype_structure.uuid in reference_uuids:
                        prototype_reference = prototype_structure
                        break
                else:
                    # If none of the prototype structures are known yet, chose the first one to be the reference
                    prototype_reference = structures[prototype[0]]
                    unique_references.append(prototype_reference)

                unique[compound][prototype_reference.uuid] = prototype_structures

            progress.update(1)

    with get_manager().get_backend().transaction():
        # Loop over all new structures, i.e. those that are in the `group_candidate`.
        for node in new_structures:
            compound = node.get_formula(mode="hill_compact")
            for prototype, duplicates in unique[compound].items():
                if node.uuid in [dup.uuid for dup in duplicates]:
                    extra = {}
                    for structure in duplicates:
                        extra.setdefault(
                            structure.get_extra("source")["database"], []
                        ).append(structure.uuid)
                    for structure in duplicates:
                        structure.set_extra("duplicates", extra)
                    break
            else:
                raise RuntimeError("this should not happen.")

    added = len(unique_references)

    if add:
        group_reference.backend_entity.add_nodes(
            [node.backend_entity for node in unique_references], skip_orm=True
        )
        echo.echo_success(
            f"Added {added} new unique references to Group<{group_reference.label}>"
        )
    else:
        echo.echo_success(f"Found {added} new unique references")
