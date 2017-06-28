"""A setuptools based setup module.

See:
https://packaging.python.org/en/latest/distributing.html
https://github.com/pypa/sampleproject
"""

# Always prefer setuptools over distutils
from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='sample',

    version='0.0.1',
    description='Hack and Craft data project helpers',
    long_description=long_description,
    url='https://github.com/HackandCraft',
    author='HNC',
    author_email='info@hackandcraft.com',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.5',
    ],

    keywords='data pyramid',

    packages=find_packages(exclude=['contrib', 'docs', 'tests']),
    install_requires=['pyramid', 'requests', 'boto3'],
    extras_require={
        'dev': ['check-manifest'],
        'test': ['coverage'],
    },

    entry_points={},
)
