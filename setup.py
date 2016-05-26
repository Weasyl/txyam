#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name="txyam2",
    version="0.5.1+weasyl.1",
    description="Yet Another Memcached (YAM) client for Twisted.",
    author="Brian Muller",
    author_email="bamuller@gmail.com",
    license="MIT",
    url="http://github.com/bmuller/txyam",
    packages=find_packages(),
    install_requires=[
        'twisted>=12.0',
        'consistent_hash',
    ],
    extras_require={
        'sync': [
            'crochet>=1.2.0',
        ],
    },
)
