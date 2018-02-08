from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import argparse
import getpass
import pandas
import synapseclient
from annotator import schema


def synapseLogin():
    """
    First tries to login to synapse by finding the local auth key cached on user's computing platform, if not found,
    prompts the user to provide their synapse user name and password, then caches the auth key on their computing
    platform.

    :return:
    """
    try:
        syn = synapseclient.login()
    except Exception as e:
        print('Please provide your synapse username/email and password (You will only be prompted once)')
        username = input("Username: ")
        password = getpass.getpass(("Password for " + username + ": ").encode('utf-8'))
        syn = synapseclient.login(email=username, password=password, rememberMe=True)

    return syn


def buildParser():
    """

    :return:
    """
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(title='commands',
                                       description='The following commands are available:',
                                       help='For additional help: "annotator <COMMAND> -h"')

    parser_meltjson = subparsers.add_parser('meltjson', help='Creates a flattened synapse table from json files '
                                                             'located on Sage-Bionetworks/synapseAnnotations/data.')

    parser_meltjson.add_argument('--tableId', help='A table synapse id containing the annotations', required=False,
                                 type=str)
    parser_meltjson.add_argument('--releaseVersion', help='Sage-Bionetworks/synapseAnnotations release version tag name',
                                 required=False, type=str)
    parser_meltjson.set_defaults(func=parser_meltjso)

    return parser


