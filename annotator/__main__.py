from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from future.utils import iteritems
from six.moves.urllib.parse import urlparse
from six.moves.urllib.request import urlopen
import subprocess
import os
import sys
import json
import six
import argparse
import getpass
import pandas
import synapseutils
import synapseclient
from synapseclient import Entity, Project, Column, Team, Wiki, Folder
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


def json2table(args, syn):
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

        elif d['columnType'] == 'BOOLEAN':
            d['maximumSize'] = 5

        elif d['columnType'] == 'DOUBLE':
            d['maximumSize'] = 20

        else:
            d['maximumSize'] = 50

        cols.append(synapseclient.Column(**d))

    return cols


def emptyView(args, syn):
    """
    Given synapse scopes, creates an empty project/file view schema to be annotated.

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

    # get default columns, check allowed column length, if max <= 60, create schema and print the saved schema
    minimal_view_schema_columns = [x['id'] for x in syn.restGET("/column/tableview/defaults/file")['list']]

    if default_columns:
        condition = len(cols) + len(minimal_view_schema_columns)
    else:
        condition = len(cols)

    if condition <= 60:
        view = syn.store(synapseclient.EntityViewSchema(name=view_name, parent=project_id, scopes=scopes, columns=cols,
                                                         addDefaultViewColumns=default_columns, addAnnotationColumns=True,
                                                         view_type=viewType))
        print(view)
    else:
        print('Please provide less than 60 columns')


def _getLists(local_root, depth):
    """
    Given a depth, creates a list of directory and files hierarchy paths.

    :param local_root:
    :param depth:
    :return:
    """
    dir_list = []
    file_list = []

    for dirpath, _, filenames in os.walk(local_root):

        sub_dir = dirpath[len(local_root):]
        n = sub_dir.count(os.path.sep) + 1 if sub_dir != '' else 0
        dirpath = os.path.abspath(dirpath)

        if depth is not None:
            if n < depth:
                dir_list.append(dirpath)
        else:
            dir_list.append(dirpath)

        for name in filenames:
            if not name.startswith('.'):
                file_list.append(os.path.join(dirpath, name))

    return dir_list, file_list


def _getSynapseDir(syn, synapse_id, local_root, dir_list):
    """
    1. Walks through Synapse parent location hierarchy.
    2. update folders in Synapse to match the local dir,
    3. get key-value pairs of dirname and synapse id

    :param syn:
    :param synapse_id:
    :param local_root:
    :param dir_list:
    :return:
    """
    synapse_dir = {}
    synapse_root = syn.get(synapse_id)

    for (dirpath, dirpath_id), _, _ in synapseutils.walk(syn, synapse_id):
        dirpath = dirpath.replace(synapse_root.name, os.path.abspath(local_root))
        synapse_dir[dirpath] = dirpath_id

    for directory in dir_list:

        if not synapse_dir.has_key(directory):
            new_folder = synapseclient.Folder(os.path.basename(directory),
                                              synapse_dir[os.path.dirname(directory)])
            new_folder = syn.store(new_folder)
            synapse_dir[directory] = new_folder.id

    return synapse_dir


def _getAnnotationKey(dirs):
    """
     Get the list of annotation keys (manifest columns)

    :param dirs:
    :return:
    """
    key_list = ['used', 'executed']

    if dirs is not None:

        for directory in dirs:

            if urlparse(directory).scheme != '':
                jfile = urlopen(directory)
            else:
                jfile = open(directory, 'r')

            base, ext = os.path.splitext(os.path.basename(directory))

            if ext == '.json':
                data = json.load(jfile)
            else:
                sys.stderr.write('File %s cannot be parsed. JSON format is required.\n' % directory)

            data = pandas.DataFrame(data)
            annotation_key = data['name']
            key_list = key_list + list(annotation_key)

    return key_list


def _getName(path, synapse_dir, local_root, depth):
    """
    Finds the name of files in local directory.

    :param path:
    :param synapse_dir:
    :param local_root:
    :param depth:
    :return: name of file and it's associated parent location/benefactor
    """
    path_no_root = path[len(os.path.abspath(local_root)):]

    if depth is not None and path_no_root.count(os.path.sep) > depth - 1:

        if str.startswith(path_no_root, '/'):
            path_no_root = path_no_root[1:]

        temp_name = path_no_root.split('/')[(depth - 1):]
        name = '_'.join(temp_name)

        temp_name = '/'.join(temp_name)
        parent = synapse_dir[os.path.dirname(path[:-len(temp_name)])]
    else:
        name = os.path.basename(path)
        parent = synapse_dir[os.path.dirname(path)]

    return name, parent


def create_sync_manifest(file_list, key_list, synapse_dir, local_root, depth):
    """
    Creates manifest designed for the input of sync function.

    :param file_list:
    :param key_list:
    :param synapse_dir:
    :param local_root:
    :param depth:
    :return:
    """
    result = pandas.DataFrame()
    result['path'] = file_list
    result['name'] = ""
    result['parent'] = ""

    for index, row in result.iterrows():
        row[['name', 'parent']] = _getName(row['path'], synapse_dir, local_root, depth)

    cols = list(result.columns)
    result = pandas.concat([result, pandas.DataFrame(columns=key_list)])
    result = result[cols + key_list] # reorder the columns
    save_path = os.path.join(os.getcwd(),'annotations_manifest.csv')
    result.to_csv(save_path, index=False)
    sys.stderr.write('Manifest has been created on local directory: \n %s \n' % os.getcwd())


def sync_manifest(args, syn):
    """
    Creates a manifest (filepath by annotations) designed for the input of synapse sync
    function to facilitate file organization and annotations of those files on synapse.

    :param args:
    :param syn:
    :return:
    """
    sys.stderr.write('Preparing to create manifest\n')
    local_root = args.directory
    synapse_id = args.id
    annotations = args.files
    depth = args.n

    if depth is not None:
        depth = int(depth)

    dir_list, file_list = _getLists(local_root, depth)
    synapse_dir = _getSynapseDir(syn, synapse_id, local_root, dir_list)
    key_list = _getAnnotationKey(annotations)

    create_sync_manifest(file_list, key_list, synapse_dir, local_root, depth)


def buildParser():
    """
    Builds the user-input argument parser.

    :return:
    """
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(title='commands',
                                       description='The following commands are available:',
                                       help='For additional help: "annotator <COMMAND> -h"')

    parser_json2table = subparsers.add_parser('json2table', help='Creates a flattened synapse table from json files '
                                                             'located on Sage-Bionetworks/synapseAnnotations/data.')
    parser_json2table.add_argument('--tableId', help='A table synapse id containing the annotations', required=False,
                                 type=str)
    parser_json2table.add_argument('--releaseVersion',
                                 help='Sage-Bionetworks/synapseAnnotations release version tag name',
                                 required=False, type=str)
    parser_json2table.set_defaults(func=json2table)

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
    parser_syncmanifest.set_defaults(func=sync_manifest)

    return parser


def _annotator_error_msg(ex):
    """
    Formats a human readable error message.

    :param ex:
    :return:
    """
    if isinstance(ex, six.string_types):
        return ex

    return '\n' + ex.__class__.__name__ + ': ' + str(ex) + '\n\n'


def performMain(args, syn):
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
    syn = synapseLogin()

    performMain(args, syn)


if __name__ == "__main__":
    main()
