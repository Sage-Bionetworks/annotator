import pandas as pd
import synapseclient as sc
import re

def synread(syn_, synId, sortCols=True):
    """ A simple way to read in Synapse entities to pandas.DataFrame objects.

    Parameters
    ----------
    syn_ : synapseclient.Synapse
    synId : str or list
        Synapse entity to read or a list of Synapse entities to concatenate
        column-wise.
    sortCols : bool
        Optional. Whether to sort columns lexicographically. Defaults to True.

    Returns
    -------
    A pandas.DataFrame object.
    """
    #if "syn" in globals(): syn_ = syn
    if isinstance(synId, str):
        f = syn_.get(synId)
        d = _synread(synId, f, syn_, sortCols)
        if hasattr(d, 'head'): print(d.head())
        if hasattr(d, 'shape'): print("Full size:", d.shape)
    else: # is list-like
        files = list(map(syn_.get, synId))
        d = [_synread(synId_, f, syn_, sortCols) for synId_, f in zip(synId, files)]
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
        d = q.asDataFrame();
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
    val_length = map(lambda v : len(v) if v else 50, values)
    cols = [{'name': k, 'maximumSize': l,
        'columnType': "STRING", "defaultValue": v}
            for k, v, l in zip(keys, values, val_length)]
    if asSynapseCols: cols = list(map(sc.Column, cols))
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
    f = pd.read_csv(fromFile, header=None)
    return _keyValCols(f[0].values, f[1].values, asSynapseCols)

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
        Optional. Whether to return as synapseclient.Column objects. Defaults to True.

    Returns
    -------
    A list of dictionaries compatible with synapseclient.Column objects.
    """
    if isinstance(obj, str): return _colsFromFile(obj, asSynapseCols)
    elif isinstance(obj, dict): return _colsFromDict(obj, asSynapseCols)
    elif isinstance(obj, list): return _colsFromList(obj, asSynapseCols)

def combineSynapseTabulars(syn, tabulars, axis=0):
    """ Concatenate tabular files.

    Parameters
    ----------
    syn : synapseclient.Synapse
    tabulars : list
        A list of Synapse IDs referencing delimited files to combine column-wise.

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
        v = v[pd.notnull(v)] # filter out na values
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
    else:
        raise TypeError("{} is not a supported referenceList type".format(
            type(referenceList)))
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
        if not m and isinstance(s, str): print("{} does not match regex.".format(s))
        newCol.append(m.group(1)) if m else newCol.append(None)
    return newCol
