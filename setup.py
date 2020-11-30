#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'Click>=7.0',
    'networkx>=2.4',
    'numpy>=1.18',
    'scipy>=1.4.0',
    'smart_open'
]

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest>=3', ]

setup(
    author="Software Engineering Laboratory of Fudan University",
    author_email='lmwtclmwtc@outlook.com',
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="a python library for create and operate on a knowledge graph in disk/neo4j/mysql.",
    entry_points={
        'console_scripts': [
            'kgdt=kgdt.cli:main',
        ],
    },
    install_requires=requirements,
    license="MIT license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='kgdt',
    name='kgdt',
    packages=find_packages(include=['kgdt', 'kgdt.*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/FudanSELab/kgdt',
    version='0.2.0',
    zip_safe=False,
)
