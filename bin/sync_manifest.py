#!/usr/bin/env python
"""
Create Synapse sync manifest
"""
import os
import sys
import json
import urlparse
import urllib
import pandas

import synapseclient
import synapseutils


def _getLists(local_root, depth):
    # walk through local directory
    # get a list of dirs and a list of files

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
    # walk through Synapse
    # update folders in Synapse to match the local dir
    # get key-value pairs of dirname and synapse id

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
    # get a list of annotation keys
    key_list = ['used', 'executed']

    if dirs is not None:
        for directory in dirs:
            if urlparse.urlparse(directory).scheme != '':
                jfile = urllib.urlopen(directory)
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


def create(file_list, key_list, synapse_dir, local_root, depth, tab):
    # create manifest

    result = pandas.DataFrame()
    result['path'] = file_list
    result['name'] = ""
    result['parent'] = ""

    for index, row in result.iterrows():
        row[['name', 'parent']] = _getName(row['path'], synapse_dir, local_root, depth)

    cols = list(result.columns)

    result = pandas.concat([result, pandas.DataFrame(columns=key_list)])
    # reorder the columns
    result = result[cols + key_list]

    if tab:
        # cat the tab delaminated manifest into sys.stdout for piping
        result.to_csv(sys.stdout, sep="\t", index=False)
    else:
        result.to_csv('annotations_manifest.csv', sep=",", index=False)
        sys.stderr.write('Manifest has been created. \n')


def main():
    import argparse
    syn = synapseclient.login(silent=True)

    parser = argparse.ArgumentParser(description="Create Synapse sync manifest")
    parser.add_argument('-d', '--directory', help='local directory')
    parser.add_argument('--id', help='Synapse ID of the project/folder')
    parser.add_argument('-f', '--files',
                        help='Path(s) to JSON file(s) of annotations (optional)',
                        nargs='+')
    parser.add_argument('-n', '--n', help='depth of hierarchy (default: %{default})',
                        default=None)
    parser.add_argument('--tab', action='store_true', help='tab delaminated manifest will be into standard output for '
                                                           'piping')

    args = parser.parse_args()

    sys.stderr.write('Preparing to create manifest\n')
    local_root = args.directory
    synapse_id = args.id
    annotations = args.files
    depth = args.n
    tab = args.tab

    if depth is not None:
        depth = int(depth)

    dir_list, file_list = _getLists(local_root, depth)

    synapse_dir = _getSynapseDir(syn, synapse_id, local_root, dir_list)
    key_list = _getAnnotationKey(annotations)

    create(file_list, key_list, synapse_dir, local_root, depth, tab)

if __name__ == '__main__':
    main()
