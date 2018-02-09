from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from future.utils import iteritems
from six.moves.urllib.parse import urlparse
from six.moves.urllib.request import urlopen
import os
import sys
import json
import urllib
import six
import argparse
import getpass
import pandas
import synapseclient
from synapseclient import Entity, Project, Column, Team, Wiki
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


def updateTable(syn, tableSynId, newTable, releaseVersion):
    """
    Gets the current annotation table, deletes all it's rows, then updates the table with new content generated
    from all the json files on synapseAnnotations. In the process also updates the table annotation to the latest
    releaseversion.

    :param syn:
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


def meltjson(args, syn):
    """
    Given a synapse table id with the schema
    annotation_schema = ["key", "description", "columnType", "maximumSize", "value", "valueDescription",
                         "source", "module"]
    get the most updated release version annotations json files from github Sage-Bionetworks/synapseAnnotations
    normalize the json files per module and create a melted data frame by concatenating all the modules data.
    then upload the melted data frame to the synapse table by completely deleting all rows then replacing content.
    This process also updates the synapse table annotations with the latest release version.

    :param args:
    :param syn:
    :return:
    """

    if args.tableId is not None:
        tableSynId = args.tableId
    else:
        tableSynId = "syn10242922"

    if args.releaseVersion is not None:
        releaseVersion = args.releaseVersion
    else:
        releaseVersion = schema.getAnnotationsRelease()

    all_modules = []
    key = ["key", "value", "module"]
    annotation_schema = ["key", "description", "columnType", "maximumSize", "value", "valueDescription",
                         "source", "module"]

    names = schema.moduleJsonPath(releaseVersion)

    for module, path in iteritems(names):
        module_df = schema.flattenJson(path, module)
        all_modules.append(module_df)

    # concat the list of all normalized dataframes into one annotation dataframe
    all_modules_df = pandas.concat(all_modules)

    # re-arrange columns/fields and sort data.
    all_modules_df = all_modules_df[annotation_schema]
    all_modules_df.sort_values(key, ascending=[True, True, True], inplace=True)
    all_modules_df.valueDescription = all_modules_df.valueDescription.str.encode('utf-8')

    updateTable(syn, tableSynId=tableSynId, newTable=all_modules_df, releaseVersion=releaseVersion)


def createColumnsFromJson(path, defaultMaximumSize=250):
    """
    Create a list of Synapse Table Columns from a Synapse annotations JSON file.
    This creates a list of columns; if the column is a 'STRING' and
    defaultMaximumSize is specified, change the default maximum size for that
    column.

    :param json_file:
    :param defaultMaximumSize:
    :return:
    """
    with open(path) as json_file:
        data = json.load(json_file)

    cols = []
    for d in data:
        d['enumValues'] = [a['value'] for a in d['enumValues']]

        if d['columnType'] == 'STRING' and defaultMaximumSize:
            d['maximumSize'] = defaultMaximumSize

        cols.append(synapseclient.Column(**d))

    return cols


def emptyView(args, syn):
    """

    :param args:
    :param syn:
    :return:
    """
    project_id = args.id
    scopes = args.scopes
    json_files = args.json
    view_name = args.name
    default_columns = args.add_default_columns

    if args.viewType:
        viewType = args.viewType
    else:
        viewType = 'file'

    if ',' in scopes:
        scopes = scopes.split(',')

    # create synapse columns from annotations json file
    cols = []
    [cols.extend(createColumnsFromJson(j)) for j in json_files]

    # create schema and print the saved schema
    view = syn.store(synapseclient.EntityViewSchema(name=view_name, parent=project_id, scopes=scopes, columns=cols,
                                                     addDefaultViewColumns=default_columns, addAnnotationColumns=True,
                                                     view_type=viewType))
    print(view)


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
    parser_meltjson.add_argument('--releaseVersion',
                                 help='Sage-Bionetworks/synapseAnnotations release version tag name',
                                 required=False, type=str)
    parser_meltjson.set_defaults(func=meltjson)

    parser_emptyview = subparsers.add_parser('emptyview', help='Given synapse scopes, creates empty project/file view '
                                                               'schema to be annotated')

    parser_emptyview.add_argument('--id', help='Synapse id of the project in which to create project/file view',
                                  required=True)
    parser_emptyview.add_argument('--name', help='Name of the project/file view to be created', required=True)
    parser_emptyview.add_argument('--scopes', nargs='+',
                                  help='One to many synapse folder or project ids that the file view should include.',
                                  required=True)
    parser_emptyview.add_argument('--add_default_columns', action='store_true',
                                  help='Add default columns to file view.', required=False)
    parser_emptyview.add_argument('--json', nargs='+',
                                  help='One or more json files to use to define the project/file view schema.',
                                  required=True)
    parser_emptyview.add_argument('--viewType', help='Type of scopes to be organized are project or file. default is '
                                                     'set to be file', required=False)
    parser_emptyview.set_defaults(func=emptyView)

    parser_syncmanifest = subparsers.add_parser('sync_manifest', help='Creates a manifest (filepath by annotations) '
                                                                      'designed for the input of synapse sync '
                                                                      'function to facilitate file organization and '
                                                                      'annotations of those files on synapse.')
    parser_syncmanifest.add_argument('-d', '--directory', help='local directory with files and folders hierarchy to '
                                                               'be mirrored on synapse.', required=True)
    parser_syncmanifest.add_argument('--id', help='Project/folder synapse id that will mirror the file organization '
                                                  'hierarchy. This information would be placed in manifest parent '
                                                  'column and would be used to allocate the parent directory on '
                                                  'synapse after sync function has been run.',
                        required=True)
    parser_syncmanifest.add_argument('-f', '--files',
                        help='Path(s) to JSON file(s) of annotations (optional)', nargs='+', required=False)
    parser_syncmanifest.add_argument('-n', '--n', help='Depth of hierarchy (default: %{default}) or number of nested '
                                                       'folders to mirror. Any file/folder beyond this number would '
                                                       'be expanded into the hierarchy number indicated.',
                                     default=None, required=False)
    parser_syncmanifest.add_argument('--tab', action='store_true', help='tab delaminated manifest will be echoed into '
                                                                        'standard output for piping', required=False)
    parser_syncmanifest.set_defaults(func=sync_manifest)

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
                sys.stderr.write(_annotator_error_msg(ex))


def main():
    args = buildParser().parse_args()
    print(args)
    syn = synapseLogin()

    performMain(args, syn)


if __name__ == "__main__":
    main()
