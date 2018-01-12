from __future__ import unicode_literals
import os
import requests
import json
import pandas as pd
from . import utils


def getAnnotationsRelease():
    """

    Returns
    -------
    the latest release version of Sage Bionetworks annotations on github
    """
    reqRelease = requests.get("https://api.github.com/repos/Sage-Bionetworks/synapseAnnotations/releases")
    releaseVersion = reqRelease.json()[0]['tag_name']

    return releaseVersion


def moduleJsonPath(releaseVersion=None):
    """ get and load the list of json files from data folder (given the api endpoint url - ref master - latest vesion)
     then construct a dictionary of module names and its associated raw data github url endpoints.

    Parameters
    ----------
    releaseVersion : str
    Optional. github release version of annotations

    Returns
    -------
    Python dictionary of module name keys and the release version path to its module raw github json URL as values

    example {u'analysis':
             u'https://raw.githubusercontent.com/Sage-Bionetworks/synapseAnnotations/master/synapseAnnotations/data/analysis.json',
            ... } @kenny++
    """
    if releaseVersion is None:
        releaseVersion = getAnnotationsRelease()

    gitPath = 'https://api.github.com/repos/Sage-Bionetworks/synapseAnnotations/contents/synapseAnnotations/data/?ref='
    req = requests.get(gitPath + releaseVersion)
    file_list = json.loads(req.content)
    names = {os.path.splitext(x['name'])[0]: x['download_url'] for x in file_list}

    return names


def flattenJson(path, module=None):
    """Normalize semi-structured JSON schema data into a flat table.

    Parameters
    ----------
    path : str
        Path to JSON file. Can be a url or filepath.
    module : str
        Optional. Module from which json schema is derived from.

    Returns
    -------
    pd.DataFrame
    """
    json_record = pd.read_json(path)

    # grab annotations with empty enumValue lists
    # i.e. don't require normalization and structure their schema
    empty_vals = json_record.loc[json_record.enumValues.str.len() == 0]
    empty_vals = empty_vals.drop('enumValues', axis=1)
    empty_vals['valueDescription'] = ""
    empty_vals['source'] = ""
    empty_vals['value'] = ""
    empty_vals['module'] = module
    empty_vals.set_index(empty_vals['name'], inplace=True)

    # for each value list object
    flatten_vals = []
    json_record = json_record.loc[json_record.enumValues.str.len() > 0]
    json_record.reset_index(inplace=True)

    for i, jsn in enumerate(json_record['enumValues']):
        normalized_values_df = pd.io.json.json_normalize(jsn)

        # re-name 'description' defined in dictionary to valueDescription
        # to match table on synapse schema
        normalized_values_df = normalized_values_df.rename(
                columns={'description': 'valueDescription'})

        # grab key information in its row, expand it by values dimension
        # and append its key-columns to flattened values
        rows = json_record.loc[[i], json_record.columns != 'enumValues']
        repeats = pd.concat([rows] * len(normalized_values_df.index))
        repeats.set_index(normalized_values_df.index, inplace=True)
        flatten_df = pd.concat([repeats, normalized_values_df], axis=1)
        # add column module for annotating the annotations
        flatten_df['module'] = module
        flatten_df.set_index(flatten_df['name'], inplace=True)
        flatten_vals.append(flatten_df)

    flatten_vals.append(empty_vals)
    module_df = pd.concat(flatten_vals)
    module_df = module_df.rename(columns={'name': 'key'})
    return module_df


def validateView(view, schema, syn=None):
    """ Check that the values in a view conform with a schema.

    Parameters
    ----------
    view : pandas DataFrame, str
        A DataFrame or Synapse ID -- anything that can be read by
        utils.synread.
    schema : pandas DataFrame, str
        A DataFrame in flattened schema format (see flattenJson) or
        path to .json file.
    syn : synapseclient.Synapse
        Optional. A Synapse object for retreiving `view` from Synapse.
        Defaults to None.

    Returns
    -------
    dict of malformed values.
    """
    view = utils.synread(syn, view)
    schema = flattenJson(schema) if isinstance(schema, str) else schema
    to_examine = schema.index.intersection(view.columns)
    malformed = {}
    for k in to_examine:
        allowed_vals = set(schema.loc[k].value)
        actual_vals = set(view.loc[:,k].unique())
        malformed_vals = actual_vals.difference(allowed_vals)
        if malformed_vals:
            malformed[k] = malformed_vals
    return malformed


def diff(syn, view, schema):
    """ Checks whether a pandas DataFrame could be pushed to Synapse as it is
    currently formatted. This implies the indices match up, all header columns
    are present, and string values within a column all fit within the max size
    allowable by that column.

    Parameters
    ----------
    syn : synapseclient.Synapse
    view : pandas.DataFrame
    schema : str, synapseclient.Schema
        The Synapse ID or Schema object of a file view on Synapse.

    Returns
    -------
    dict {
        'indicesMatch': bool indicating whether `view` indices are a subset
            of `schema` indices,
        'missingInSchema': list containing column names present in `view`
            but not in `schema`,
        'missingInView': list containing column names present in `schema`
            but not in `view`,
        'tooSmallCols': list containg tuples of (column name, max `view`
            value size). Only columns that have values in `view` which exceed
            the max allowable value size for the respective column in `schema`
            will be included.
        'raw': dict containing cached remote entities used during this function
            to save the caller some time in the case the entities need to be
            referenced upon finding a difference between `view` and `schema`
        }
    """
    result = {}
    synapseId = schema if isinstance(schema, str) else schema.id
    print("Fetching index...")
    df = syn.tableQuery(
            "select * from {}".format(synapseId)).asDataFrame()
    schemaIndices = df.index
    print("Getting table columns...")
    schemaCols = list(syn.getTableColumns(synapseId))
    # Check for invalid indices
    invalidIndices = view.index.difference(schemaIndices)
    result['indicesMatch'] = False if len(invalidIndices) else True
    # Check for missing header values
    schemaColNames = [c['name'] for c in schemaCols]
    missingInSchema = view.columns.difference(schemaColNames)
    result['missingInSchema'] = missingInSchema
    missingInView = list(set(schemaColNames).difference(view.columns))
    result['missingInView'] = missingInView
    result['tooSmallCols'] = []
    for c in schemaCols:
        if (c['columnType'] == 'STRING' and 'maximumSize' in c and
                c['name'] in view.columns):
            s = view[c['name']].astype(str)
            # pandas interprets int Series with None types as float.
            # This results in values that seem two chars longer than necessary.
            if (pd.np.issubdtype(view[c['name']].dtype, pd.np.float_) and
                    all(s.agg(lambda x: x == 'nan' or x[-2:] == '.0'))):
                s = pd.Series([str(int(i)) for i in view[c['name']].values
                        if pd.notnull(i)])
            # if s was entirely NaN in the conditional above it will be empty.
            biggestValue = s.str.len().max() if len(s) else -1
            if biggestValue > c['maximumSize']:
                result['tooSmallCols'].append((c['name'], biggestValue))
    result['raw'] = {'df': df, 'cols': schemaCols}
    return result
