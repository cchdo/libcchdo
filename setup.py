from __future__ import with_statement
import setuptools
import unittest
import glob
import os

import libcchdo
from libcchdo.setup_commands import (
    DIRECTORY, PACKAGE_NAME,
    CoverageCommand,
    CleanCommand,
    PurgeCommand,
    ProfileCommand,
    REPLCommand,
    )


if __name__ == "__main__":
    long_description = ''
    try:
        with open(os.path.join(DIRECTORY, 'README.txt')) as f:
            long_description = f.read()
    except IOError:
        pass

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

    # Add all the extras as requirements. Pip and setup.py develop don't support
    # extras_require.
    install_requires = reduce(
        lambda l, m: l + m, extras_require.values(), install_requires)

    setuptools.setup(
        name=PACKAGE_NAME,
        version=libcchdo.__version__,
        description="CLIVAR and Carbon Hydrographic Data Office library",
        long_description=long_description,
        provides=[PACKAGE_NAME],
        packages=['%s%s' % (PACKAGE_NAME, x) for x in (
            '', '.algorithms', '.datadir',
            '.db', '.db.model', '.formats',
            '.formats.ctd', '.formats.ctd.zip',
            '.formats.bottle', '.formats.bottle.zip',
            '.formats.common', '.formats.summary',
            '.model', '.model.convert', '.region',
            '.units', )],
        test_suite='libcchdo.tests',
        install_requires=install_requires,
        extras_require=extras_require,
        scripts=glob.glob('libcchdo/scripts/*'),
        cmdclass={
            'coverage': CoverageCommand,
            'clean': CleanCommand,
            'purge': PurgeCommand,
            'profile': ProfileCommand,
            'repl': REPLCommand,
        },
    )
