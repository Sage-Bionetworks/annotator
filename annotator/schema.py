from __future__ import unicode_literals
import pandas as pd


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

    # grab annotations with empty enumValue lists i.e. don't require normalization and structure their schema
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

        # re-name 'description' defined in dictionary to valueDescription to match table on synapse schema
        normalized_values_df = normalized_values_df.rename(columns={'description': 'valueDescription'})

        # grab key information in its row, expand it by values dimension and append its key-columns to flattened values
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


def validateView(view, schema):
    """ Check that a view conforms with a schema.

    Parameters
    ----------
    view : pandas DataFrame, str
        A DataFrame or Synapse ID -- anything that can be read by
        utils.synread.
    schema : pandas DataFrame, str
        A DataFrame in flattened schema format (see flattenJson) or
        path to .json file.

    Returns
    -------
    bool
    """
    pass
