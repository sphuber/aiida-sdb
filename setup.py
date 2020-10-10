"""Setup for the `aiida-sdb` CLI."""
import fastentrypoints  # pylint: disable=unused-import


def setup_package():
    """Setup procedure."""
    import json
    from setuptools import setup, find_packages

    with open('setup.json', 'r') as handle:
        setup_json = json.load(handle)

    setup(
        include_package_data=True,
        packages=find_packages(include=['aiida_sdb', 'aiida_sdb.*']),
        **setup_json
    )


if __name__ == '__main__':
    setup_package()
