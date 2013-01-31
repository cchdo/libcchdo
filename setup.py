from __future__ import with_statement
from setuptools import setup, find_packages
import sys
import os

import libcchdo
from libcchdo.setup_commands import (
    DIRECTORY, PACKAGE_NAME,
    CoverageCommand, CleanCommand, PurgeCommand, ProfileCommand, REPLCommand,
    )


if __name__ == "__main__":
    long_description = ''
    try:
        with open(os.path.join(DIRECTORY, 'README.txt')) as f:
            long_description = f.read()
    except IOError:
        pass

    # To install extras with pip as editable:
    # pip install -e .[speed,netcdf]
    extras_require = {
        'db': ['MySQL-python', ],
        'speed': ['cdecimal', ],
        'coverage': ['coverage', ],
        'plot': ['matplotlib', 'basemap', ],
        'netcdf': ['numpy', 'netCDF4', ],
        'merge': ['pandas', 'numpy>=1.6'],
    }

    install_requires = [
        'geoalchemy',
    ]
    if sys.version_info[:3] < (2,5,0):
        install_requires.append('pysqlite')
    if sys.version_info[:3] < (2,7,0):
        install_requires.append('argparse')

    packages = find_packages(exclude=['libcchdo.tests'])

    setup(
        name=PACKAGE_NAME,
        version=libcchdo.__version__,
        description="CLIVAR and Carbon Hydrographic Data Office library",
        long_description=long_description,
        provides=[PACKAGE_NAME],
        packages=packages,
        test_suite='libcchdo.tests',
        install_requires=install_requires,
        extras_require=extras_require,
        entry_points={
            'console_scripts': [
                'hydro = libcchdo.scripts:main',
                'reorder_surface_to_bottom = libcchdo.scripts:'
                    'deprecated_reorder_surface_to_bottom',
            ],
        },
        cmdclass={
            'coverage': CoverageCommand,
            'clean': CleanCommand,
            'purge': PurgeCommand,
            'profile': ProfileCommand,
            'repl': REPLCommand,
        },
    )
