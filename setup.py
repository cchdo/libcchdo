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
    # pip install -e .[all] will install all extras
    # pip install -e .[db,speed,netcdf]
    extras_require = {
        'csv_view': ['lxml', ],
        'db': ['MySQL-python', ],
        'datadir': ['oauth2', 'paramiko', 'docutils', ],
        'speed': ['cdecimal', ],
        'kml': ['pykml', ],
        'coverage': ['coverage', ],
        'netcdf': ['numpy', 'netCDF4', ],
        'autocomplete': ['argcomplete'],
        'merge': ['pandas', 'numpy>=1.6'],
        'plot': ['numpy>=1.4', 'scipy', 'PIL', 'matplotlib', 'basemap', ],
        'dap_thredds': ['lxml', 'httplib2', 'pydap'],
    }
    extras_require['all'] = extras_require.values()

    install_requires = [
        'geoalchemy',
    ]
    if sys.version_info[:3] < (2,5,0):
        install_requires.append('pysqlite')
    if sys.version_info[:3] < (2,7,0):
        install_requires.append('argparse')

    dependency_links = [
        'https://github.com/matplotlib/basemap/archive/v1.0.6rel.zip#egg=basemap-1.0.6',
    ]

    packages = find_packages(exclude=['libcchdo.tests'])

    resources = [os.path.join('resources', fname) for fname in
                 os.listdir(os.path.join(PACKAGE_NAME, 'resources'))]

    setup(
        name=PACKAGE_NAME,
        version=libcchdo.__version__,
        description="CLIVAR and Carbon Hydrographic Data Office library",
        long_description=long_description,
        provides=[PACKAGE_NAME],
        packages=packages,
        package_data={PACKAGE_NAME:[
            'RELEASE-VERSION.txt',
            ] + resources,
        },
        test_suite='libcchdo.tests',
        install_requires=install_requires,
        dependency_links=dependency_links,
        extras_require=extras_require,
        entry_points={
            'console_scripts': [
                'hydro = libcchdo.scripts:main',
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
