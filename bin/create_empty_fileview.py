#!/usr/bin/env python

"""Create empty file view from a Synapse annotations json file.

"""
from future.standard_library import hooks
with hooks():
    from urllib.parse import urlparse
    from urllib.parse import urljoin
import os
import sys
import json
import urllib
import six
import argparse
import synapseclient


def path2url(path):
    """Convert path to URL, even if it already is a URL.
    """
    if path.startswith("/"):
        new_path = urljoin('file:', urllib.pathname2url(os.path.abspath(path)))
    else:
        new_path = path

    return new_path


def createColumnsFromJson(json_file, defaultMaximumSize=250):
    """Create a list of Synapse Table Columns from a Synapse annotations JSON file.

    This creates a list of columns; if the column is a 'STRING' and
    defaultMaximumSize is specified, change the default maximum size for that
    column.

    """
    f = urllib.urlopen(path2url(json_file))
    data = json.load(f)

    cols = []

    for d in data:
        d['enumValues'] = [a['value'] for a in d['enumValues']]

        if d['columnType'] == 'STRING' and defaultMaximumSize:
            d['maximumSize'] = defaultMaximumSize

        cols.append(synapseclient.Column(**d))

    return cols


def main():

    parser = argparse.ArgumentParser(description="Given synapse scopes, creates empty project/file view schema to be "
                                                 "annotated")
    parser.add_argument('--id', help='Synapse id of the project in which to create project/file view', required=True)
    parser.add_argument('-n', '--name', help='Name of the project/file view to be created', required=True)
    parser.add_argument('-s', '--scopes', nargs='+', help='one to many synapse folder or project ids that the file '
                                                          'view should include.', required=True)
    parser.add_argument('--add_default_columns', action='store_true', help='Add default columns to file view.',
                        required=False)
    parser.add_argument('--json', nargs='+', help='One or more json files to use to define the project/file view '
                                                  'schema.', required=True)
    parser.add_argument('--viewType', required=False,
                        help='Type of scopes to be organized are project or file. default is set to be file')

    args = parser.parse_args()
    syn = synapseclient.login(silent=True)

    project_id = args.id
    scopes = args.scopes
    json_files = args.json
    view_name = args.name

    if args.add_default_columns:
        default_columns = args.add_default_columns
    else:
        default_columns = False

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


if __name__ == '__main__':
    main()
