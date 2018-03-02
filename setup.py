from setuptools import setup, find_packages
import sys

import requests

reqRelease = requests.get("https://api.github.com/repos/Sage-Bionetworks/annotator/releases")
reqReleaseJson = reqRelease.json()

try:
    releaseVersion = reqRelease.json()[0]['tag_name']
except KeyError as e:
    print "%s\n" % (reqReleaseJson, )
    raise e

__version__=str(releaseVersion[1:len(releaseVersion)])

setup(
    name='annotator',
    version=__version__,
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
