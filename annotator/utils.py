from __future__ import print_function
import pandas as pd
import synapseclient as sc
import re
import json


def synread(syn_, obj, silent=True, sortCols=True):
    """ A simple way to read in Synapse entities to pandas.DataFrame objects.

    Parameters
    ----------
    syn_ : synapseclient.Synapse
    obj : pd.DataFrame, str, or list
        Synapse entity to read or a list of Synapse entities to concatenate
        column-wise.
    sortCols : bool
        Optional. Whether to sort columns lexicographically. Defaults to True.

    Returns
    -------
    A pandas.DataFrame object.
    """
    # if "syn" in globals(): syn_ = syn
    if isinstance(obj, pd.DataFrame):
        obj = obj.sort_index(1) if sortCols else obj
        return obj
    elif isinstance(obj, str):
        f = syn_.get(obj)
        d = _synread(obj, f, syn_, sortCols)
        if not silent:
            if hasattr(d, 'head'):
                print(d.head())
            if hasattr(d, 'shape'):
                print("Full size:", d.shape)
    else:  # is list-like
        files = list(map(syn_.get, obj))
        d = [_synread(synId_, f, syn_, sortCols)
             for synId_, f in zip(obj, files)]
    return d


def _synread(synId, f, syn_, sortCols):
    """ See `synread` """
    if isinstance(f, sc.entity.File):
        if f.path is None:
            d = None
        else:
            d = pd.read_csv(f.path, header="infer", sep=None, engine="python")
    elif isinstance(f, (sc.table.EntityViewSchema, sc.table.Schema)):
        q = syn_.tableQuery("select * from %s" % synId)
        d = q.asDataFrame()
    if sortCols:
        return d.sort_index(1)
    else:
        return d


def clipboardToDict(sep):
    """ Parse two-column delimited clipboard contents to a dictionary.

    Parameters
    ----------
    sep : str
        A string or regex to use as delimiter.

    Returns
    -------
    A dictionary derived from clipboard contents.
    """
    df = pd.read_clipboard(sep, header=None, engine='python')
    d = {k: v for k, v in zip(df[0], df[1])}
    return d


def _keyValCols(keys, values, asSynapseCols):
    """ Get Synapse Column compatible objects from `keys` and `values`.

    Parameters
    ----------
    keys : list
        Column names.
    values : list
        `defaultValue`s of each column.
    asSynapseCols : bool
        Whether to return as synapseclient.Column objects.

    Returns
    -------
    A list of dictionaries compatible with synapseclient.Column objects.
    """
    sanitize = lambda v : v if pd.notnull(v) else ''
    keys = list(map(sanitize, keys))
    values = list(map(sanitize, values))
    val_length = map(lambda v: len(v) if v else 50, values)
    cols = [{'name': k, 'maximumSize': l,
             'columnType': "STRING", "defaultValue": v}
            for k, v, l in zip(keys, values, val_length)]
    if asSynapseCols:
        cols = list(map(lambda c: sc.Column(**c), cols))
    return cols


def _colsFromFile(fromFile, asSynapseCols):
    """ Get Synapse Column compatible objects from a filepath.

    Parameters
    ----------
    fromFile : str
        Filepath to a delimited, two-column file.
    asSynapseCols : bool
        Whether to return as synapseclient.Column objects.

    Returns
    -------
    A list of dictionaries compatible with synapseclient.Column objects.
    """
    f = pd.read_csv(fromFile, names=['keys', 'values'])
    return _keyValCols(f['keys'], f['values'], asSynapseCols)


def _colsFromDict(d, asSynapseCols):
    """ Get Synapse Column compatible objects from a dictionary.

    Parameters
    ----------
    d : dict
        A dictionary containing column name -> defaultValue pairs.
    asSynapseCols : bool
        Whether to return as synapseclient.Column objects.

    Returns
    -------
    A list of dictionaries compatible with synapseclient.Column objects.
    """
    keys = [i[0] for i in d.items()]
    values = [i[1] for i in d.items()]
    return _keyValCols(keys, values, asSynapseCols)


def _colsFromList(l, asSynapseCols):
    """ Get Synapse Column compatible objects from a list.

    Parameters
    ----------
    l : list
        A list containing column names.
    asSynapseCols : bool
        Whether to return as synapseclient.Column objects.

    Returns
    -------
    A list of dictionaries compatible with synapseclient.Column objects.
    """
    keys = l
    values = [None for i in l]
    return _keyValCols(keys, values, asSynapseCols)


def makeColumns(obj, asSynapseCols=True):
    """ Create new Synapse.Column compatible objects.

    Parameters
    ----------
    obj : str, dict, or list
        object to parse to columns.
    asSynapseCols : bool
        Optional. Whether to return as synapseclient.Column objects.
        Defaults to True.

    Returns
    -------
    A list of dictionaries compatible with synapseclient.Column objects.
    """
    if isinstance(obj, str):
        return _colsFromFile(obj, asSynapseCols)
    elif isinstance(obj, dict):
        return _colsFromDict(obj, asSynapseCols)
    elif isinstance(obj, list):
        return _colsFromList(obj, asSynapseCols)
    else:
        raise TypeError("{} is not a supported type.".format(type(obj)))


def dropColumns(syn, target, cols):
    """ Delete columns from a file view on Synapse.

    Parameters
    ----------
    syn : synapseclient.Synapse
    target : str, synapseclient.Schema
        The Synapse ID of a Synapse Table or File View, or its schema.
    cols : str, list
        A str or list of str indicating column names to drop.

    Returns
    -------
    synapseclient.table.EntityViewSchema
    """
    cols = [cols] if isinstance(cols, str) else cols
    schema = syn.get(target) if isinstance(target, str) else target
    cols_ = syn.getTableColumns(schema.id)
    for c in cols_:
        if c.name in cols:
            schema.removeColumn(c)
    schema = syn.store(schema)
    return schema


def addToScope(syn, target, scope):
    """ Add further Folders/Projects to the scope of a file view.

    Parameters
    ----------
    syn : synapseclient.Synapse
    target : str, synapseclient.Schema
        The Synapse ID of the file view to update or its schema.
    scope : str, list
        The Synapse IDs of the entites to add to the scope.

    Returns
    -------
    synapseclient.Schema
    """
    scope = [scope] if isinstance(scope, str) else scope
    target = syn.get(target) if isinstance(target, str) else target
    cols = list(syn.getTableColumns(target.id))
    totalScope = target['scopeIds']
    for s in scope:
        totalScope.append(s)
    # We need to preserve columns that are currently in the file view
    # but aren't automatically created when synapseclient.EntityViewSchema'ing.
    defaultCols = getDefaultColumnsForScope(syn, totalScope)
    defaultCols = [sc.Column(**c) for c in defaultCols]
    colNames = [c['name'] for c in cols]
    for c in defaultCols: # Preexisting columns have priority over defaults
        if c['name'] not in colNames:
            cols.append(c)
    schema = sc.EntityViewSchema(name=target.name, parent=target.parentId,
            columns=cols, scopes=totalScope, add_default_columns=False)
    schema = syn.store(schema)
    return schema


def getDefaultColumnsForScope(syn, scope):
    """ Fetches the columns which would be used in the creation
    of a file view with the given scope.

    Parameters
    ----------
    syn : synapseclient.Synapse
    scope : str, list
        The Synapse IDs of the entites to fetch columns for.

    Returns
    -------
    list of dict
    """
    scope = [scope] if isinstance(scope, str) else scope
    params = {'scope': scope, 'viewType': 'file'}
    cols = syn.restPOST('/column/view/scope',
                             json.dumps(params))['results']
    return cols


def combineSynapseTabulars(syn, tabulars, axis=0):
    """ Concatenate tabular files.

    Parameters
    ----------
    syn : synapseclient.Synapse
    tabulars : list
        A list of Synapse IDs referencing delimited files
        to combine column-wise.

    Returns
    -------
    pandas.DataFrame
    """
    tabulars = synread(syn, tabulars)
    return pd.concat(tabulars, axis=axis, ignore_index=True).sort_index(1)


def compareDicts(dict1, dict2):
    """ Compare two dictionaries, returning sets containing keys from
    dict1 difference dict2, dict2 difference dict1, and shared keys with
    non-equivalent values, respectively.

    Parameters
    ----------
    dict1 : dict
    dict2 : dict

    Returns
    -------
    set, set, set
    """
    d1_keys = set(dict1.keys())
    d2_keys = set(dict2.keys())
    new = d1_keys - d2_keys
    missing = d2_keys - d1_keys
    modified = {k for k in d1_keys & d2_keys if dict1[k] != dict2[k]}
    return new, missing, modified


def inferValues(df, col, referenceCols):
    """ Fill in values for indices which match on `referenceCols`
    and which have a single, unique, non-NaN value in `col`.

    Parameters
    ----------
    df : pd.DataFrame
    col : str
        Column to fill with values.
    referenceCols : list or str
        Column(s) to match on.

    Returns
    -------
    pd.DataFrame
    """
    df = df.copy()
    groups = df.groupby(referenceCols)
    values = groups[col].unique()
    for k, v in values.items():
        v = v[pd.notnull(v)]  # filter out na values
        if len(v) == 1:
            df.loc[df[referenceCols] == k, col] = v[0]
        else:
            print("Unable to infer value when {} = {}".format(
                referenceCols, k))
    return df


def substituteColumnValues(referenceList, mod):
    """ Substitute values in a column according to a mapping.

    Parameters
    ----------
    referenceList : list
        The values to substitute.
    mod : dict
        Mappings from the old to new values.
    """
    if isinstance(mod, dict):
        referenceList = [mod[v] if v in mod else v for v in referenceList]
    return referenceList


def colFromRegex(referenceList, regex):
    """ Return a list created by mapping a regular expression to another list.
    The regular expression must contain at least one capture group.

    Parameters
    ----------
    referenceList : list-like
        A list to derive new values from.
    regex : str
        A regular expression to be applied.

    Returns
    -------
    The list resulting from mapping `regex` to `referenceList`.
    """
    p = re.compile(regex)
    if not p.groups:
        raise RuntimeError("`regex` must have at least one capture group.")
    newCol = []
    for s in referenceList:
        m = p.search(s) if isinstance(s, str) else None
        if not m and isinstance(s, str):
            print("{} does not match regex.".format(s))
        newCol.append(m.group(1)) if m else newCol.append(None)
    return newCol
