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


def updateTable(tableSynId, newTable, releaseVersion):
    """
    Gets the current annotation table, deletes all it's rows, then updates the table with new content generated
    from all the json files on synapseAnnotations. In the process also updates the table annotation to the latest
    releaseversion.

    :param tableSynId:
    :param newTable:
    :param releaseVersion:
    :return:
    """

    currentTable = syn.tableQuery("SELECT * FROM %s" % tableSynId)

    # If current table has rows, delete all the rows
    if currentTable.asRowSet().rows:
        deletedRows = syn.delete(currentTable.asRowSet())

    # get table schema and set it's release version annotation
    tableSchema = syn.get(tableSynId)
    tableSchema.annotations = {"annotationReleaseVersion": str(releaseVersion)}
    updated_schema_release = syn.store(tableSchema)

    # store the new table on synapse
    table = syn.store(synapseclient.Table(tableSchema, newTable))


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


def _annotator_error_msg(ex):
    """
    Format a human readable error message

    :param ex:
    :return:
    """
    if isinstance(ex, six.string_types):
        return ex

    return '\n' + ex.__class__.__name__ + ': ' + str(ex) + '\n\n'


def performMain(args, syn):
    """


    :param args:
    :param syn:
    :return:
    """
    if 'func' in args:
        try:
            args.func(args, syn)
        except Exception as ex:
            if args.debug:
                raise
            else:
                sys.stderr.write(_csbc_error_msg(ex))


def main():
    args = buildParser().parse_args()
    syn = synapseLogin()

    performMain(args, syn)


if __name__ == "__main__":
    main()
