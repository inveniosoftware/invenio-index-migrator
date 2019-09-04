# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2019 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Invenio module for Elasticsearch index migration."""

import os

from setuptools import find_packages, setup

readme = open('README.rst').read()
history = open('CHANGES.rst').read()

tests_require = [
    "check-manifest>=0.35",
    "coverage>=4.4.1",
    "isort>=4.3.11",
    "mock>=2.0.0",
    "pydocstyle>=2.0.0",
    "pytest-cov>=2.5.1",
    "pytest-invenio>=1.1.0,<1.2.0",
    "pytest-mock>=1.6.0",
    "pytest-pep8>=1.0.6",
    "pytest-random-order>=0.5.4",
    "pytest>=3.8.1,<4",
    "invenio-app>=1.2.0",
]

invenio_search_version = '1.2.0'

extras_require = {
    'docs': [
        'Sphinx>=1.5.6,<1.6',
    ],
    'elasticsearch5': [
        'invenio-search[elasticsearch5]>={}'.format(invenio_search_version),
    ],
    'elasticsearch6': [
        'invenio-search[elasticsearch6]>={}'.format(invenio_search_version),
    ],
    'elasticsearch7': [
        'invenio-search[elasticsearch7]>={}'.format(invenio_search_version),
    ],
    'tests': tests_require,
}

extras_require['all'] = []
for name, reqs in extras_require.items():
    if name[0] == ':' or name in ('elasticsearch5', 'elasticsearch6', 'elasticsearch7'):
        continue
    extras_require['all'].extend(reqs)

setup_requires = [
    'pytest-runner>=2.6.2',
]

install_requires = [
    "invenio-db>=1.0.0",
    "invenio-records>=1.3.0",
    "invenio-indexer>=1.1.0",
    "invenio-pidstore>=1.0.0"
]

packages = find_packages()

# Get the version string. Cannot be done with import!
g = {}
with open(os.path.join('invenio_index_migrator', 'version.py'), 'rt') as fp:
    exec(fp.read(), g)
    version = g['__version__']

setup(
    name='invenio-index-migrator',
    version=version,
    description=__doc__,
    long_description=readme + '\n\n' + history,
    keywords='invenio search',
    license='MIT',
    author='CERN',
    author_email='info@inveniosoftware.org',
    url='https://github.com/inveniosoftware/invenio-index-migrator',
    packages=packages,
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    entry_points={
        'invenio_base.api_apps': [
            'invenio_index_migrator = invenio_index_migrator:InvenioIndexMigrator',
        ],
        'invenio_base.apps': [
            'invenio_index_migrator = invenio_index_migrator:InvenioIndexMigrator',
        ],
    },
    extras_require=extras_require,
    install_requires=install_requires,
    setup_requires=setup_requires,
    tests_require=tests_require,
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: Implementation :: CPython',
        'Development Status :: 5 - Production/Stable',
    ],
)
