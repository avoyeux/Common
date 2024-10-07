"""
To setup and install the Common package.
"""

# Imports
import os
from setuptools import setup, find_packages


def load_requirements(
        filepath=os.path.join(os.getcwd(), 'requirements.txt')
    ) -> list[str]:
    with open(filepath, 'r') as requirements: installs = requirements.read().splitlines()
    return installs  

setup(
    name='Common',
    version='0.0.1',
    author='Voyeux Alfred',
    author_email='alfredvoyeux@hotmail.com',
    packages=find_packages(exclude=['tests', 'tests.*']),
    install_requires=load_requirements(),
    description='Some functions that I use a lot.',
)

