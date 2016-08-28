#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name="txyam2",
    version="0.5.1+weasyl.1",
    description="Yet Another Memcached (YAM) client for Twisted.",
    author="Brian Muller",
    author_email="bamuller@gmail.com",
    license="MIT",
    url="https://github.com/Weasyl/txyam2",
    packages=find_packages(),
    install_requires=[
        'twisted>=16.4',
        'consistent_hash_git==0.3',
    ],
    extras_require={
        'sync': [
            'crochet>=1.2.0',
        ],
    },
)
