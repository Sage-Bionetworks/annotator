from setuptools import setup, find_packages

setup(
    name='annotator',
    version='0.1',
    description='A Synapse annotations automation tool.',
    url='https://github.com/Sage-Bionetworks/annotator',
    author='Phil Snyder, Nasim Sanati',
    author_email='phil.snyder@sagebase.org',
    license='MIT',
    packages=find_packages(),
    scripts=['bin/json2synapse.py',
             'bin/create_empty_fileview.py',
             'bin/sync_manifest.py'],
    install_requires=[
        'pandas',
        'synapseclient'])
