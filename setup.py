from setuptools import setup, find_packages

setup(
    name='annotator',
    version='0.1',
    description='A Synapse annotations automation tool.',
    url='https://github.com/Sage-Bionetworks/annotator',
    author='Phil Snyder',
    author_email='phil.snyder@sagebase.org',
    license='MIT',
    packages=find_packages(),
    install_requires=[
        'pandas',
        'synapseclient'])
