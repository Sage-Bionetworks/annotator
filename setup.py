from setuptools import setup, find_packages
import requests

setup(
    name='annotator',
    version='2.1.1-dev',
    description='A Synapse annotations automation tool.',
    url='https://github.com/Sage-Bionetworks/annotator',
    author='Phil Snyder, Nasim Sanati',
    author_email='phil.snyder@sagebase.org',
    license='MIT',
    packages=find_packages(),
    entry_points={
        'console_scripts': ['annotator = annotator.__main__:main']
    },
    install_requires=[
        'pandas',
        'synapseclient'],
    tests_require=['pytest'])
