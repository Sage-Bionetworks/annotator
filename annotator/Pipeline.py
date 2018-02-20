from __future__ import print_function
import pandas as pd
import synapseclient as sc
import readline
from . import utils
from . import schema as schemaModule
from copy import deepcopy


class Pipeline:
    """ Annotations pipeline object. """

    BACKUP_LENGTH = 50

    def __init__(self, syn, view=None, meta=None, activeCols=[],
                 metaActiveCols=[], links=None, sortCols=True, schema=None):
        """ Create a new Pipeline object.

        Parameters
        ----------
        syn : synapseclient.Synapse
            Synapse object to communicate with Synapse.org.
        view : str or pandas.DataFrame
            Optional. The "data". If a str, needs to be the Synapse ID of
            a file view or table. Defaults to `None`.
        meta : str, list, or pandas.DataFrame
            Optional. The "metadata". If a str, can be the Synapse ID of
            a file view, table, or other delimited file. If a list, will
            concatenate the results of each utils.synread by column.
            Defaults to `None`.
        activeCols : str, list, dict, or pandas.DataFrame
            Optional. Active columns to add. (See `self.addActiveColumns`).
            Defaults to the empty list.
        metaActiveCols : str, list, dict, or pandas.DataFrame
            Optional. Active columns to add for the metadata.
            (See `self.addActiveColumns`). Defaults to the empty list.
        links : dict
            Optional. Link values to add. (see `self.addLinks` or
            `self.linkMetadata`). Defaults to `None`.
        sortCols : bool
            Optional. Whether to sort the columns lexicographically in
            `view` and/or `meta`. Defaults to True.
        """
        self.syn = syn
        self.view = view if view is None else self._parseView(view, sortCols)
        self._entityViewSchema = (self.syn.get(view)
                                  if isinstance(view, str) else None)
        self.schema = (schemaModule.flattenJson(schema)
                       if isinstance(schema, str) else schema)
        self._index = self.view.index if isinstance(
                self.view, pd.DataFrame) else None
        self._activeCols = []
        if activeCols:
            self.addActiveCols(activeCols, backup=False)
        self._meta = meta if meta is None else self._parseView(
                meta, sortCols, isMeta=True)
        self._metaActiveCols = []
        if metaActiveCols:
            self.addActiveCols(metaActiveCols, isMeta=True, backup=False)
        self._sortCols = sortCols
        self.keyCol = None
        self.links = links if isinstance(links, dict) else None
        self._backup = []

    def backup(self, message):
        """ Backup the state of `self` and store in `self._backup` """
        self._backup.append((Pipeline(
            self.syn, self.view, self._meta, self._activeCols,
            self._metaActiveCols, self.links, self._sortCols), message))
        if len(self._backup) > self.BACKUP_LENGTH:
            self._backup = self._backup[1:]

    def undo(self):
        """ Revert `self` to last recorded state. """
        if self._backup:
            backup, message = self._backup.pop()
            self.syn = backup.syn
            self.view = backup.view
            self._activeCols = backup._activeCols
            print("Undo: {}".format(message))
        else:
            raise IndexError("At last available change.")

    def head(self):
        """ Print head of `self.view` """
        if hasattr(self.view, 'head'):
            print(self.view.head())
        else:
            raise AttributeError("No data view set.")

    def tail(self):
        """ Print tail of `self.view` """
        if hasattr(self.view, 'tail'):
            print(self.view.tail())
        else:
            raise AttributeError("No data view set.")

    def shape(self):
        """ Print shape of `self.view` """
        if hasattr(self.view, 'shape'):
            print(self.view.shape)
        else:
            raise AttributeError("No data view set.")

    def drop(self, labels, axis):
        """ Delete rows or columns from a file view on Synapse.*
            Rows are only dropped locally. Deleting rows from a
            file view on Synapse would require deleting the file itself.
            Columns are dropped both locally and remotely on Synapse.

        Parameters
        ----------
        labels : str, list
            Can either be a str indicating the index (usually formatted
            ROWID_VERSION) or a list of str.
            axis : int
            For a two-dimensional dataframe, 0 indicates rows whereas
            1 indicates columns.

        Returns
        -------
        A list of indices deleted.
        """
        labels = [labels] if isinstance(labels, str) else labels
        if axis == 0:
            self._index = self._index.drop(labels)
        elif axis == 1:
            self._entityViewSchema = utils.dropColumns(
                    self.syn, self._entityViewSchema, labels)
            if isinstance(self.schema, pd.DataFrame):
                self.schema = self.schema[[l not in labels
                                           for l in self.schema.key]]
        self.view = self.view.drop(labels, axis=axis)


    def metaHead(self):
        """ Print head of `self._meta` """
        if hasattr(self._meta, 'head'):
            print(self._meta.head())
        else:
            raise AttributeError("No metadata view set.")

    def metaTail(self):
        """ Print tail of `self._meta` """
        if hasattr(self._meta, 'tail'):
            print(self._meta.tail())
        else:
            raise AttributeError("No metadata view set.")

    def metaShape(self):
        """ Print shape of `self._meta` """
        if hasattr(self._meta, 'shape'):
            print(self._meta.shape)
        else:
            raise AttributeError("No metadata view set.")

    def columns(self, style="numbers"):
        """ Pretty print `self.view.columns`.

        Parameters
        ----------
        style : str
            Optional. One of 'numbers' or 'letters'. Defaults to 'numbers'.
        """
        if hasattr(self.view, 'columns'):
            self._prettyPrintColumns(self.view.columns, style)
        else:
            raise AttributeError("No data view set.")

    def metaColumns(self, style="numbers"):
        """ Pretty print `self._meta.columns`.

        Parameters
        ----------
        style : str
            Optional. One of 'numbers' or 'letters'. Defaults to 'numbers'.
        """
        if hasattr(self._meta, 'columns'):
            self._prettyPrintColumns(self._meta.columns, style)
        else:
            raise AttributeError("No metadata view set.")

    def activeColumns(self, style="numbers"):
        """ Pretty print `self._activeCols`.

        Parameters
        ----------
        style : str
            Optional. One of 'numbers' or 'letters'. Defaults to 'numbers'.
        """
        if self._activeCols:
            self._prettyPrintColumns(self._activeCols, style)
        else:
            print("No active columns.")

    def metaActiveColumns(self, style="numbers"):
        """ Pretty print `self._metaActiveCols`.

        Parameters
        ----------
        style : str
            Optional. One of 'numbers' or 'letters'. Defaults to 'numbers'.
        """
        if self._metaActiveCols:
            self._prettyPrintColumns(self._metaActiveColumns, style)
        else:
            print("No active columns.")


    def addView(self, scope):
        """ Add further Folders/Projects to the scope of `self.view`.

        Parameters
        ----------
        scope : str, list
            The Synapse IDs of the entites to add to the scope.

        Returns
        -------
        synapseclient.Schema
        """
        if self._entityViewSchema is None:
            # check entity type of scope
            scope = [scope] if isinstance(scope, str) else scope
            entities = [self.syn.get(f, downloadFile=False) for f in scope]
            if not all([isinstance(e, (sc.EntityViewSchema, sc.Schema))
                        for e in entities]):
                raise RuntimeError("Must first create a file view if the "
                                   "view is not yet set and not all items in "
                                   "the scope are File Views or Schemas.")
        self._entityViewSchema = utils.addToScope(self.syn,
                self._entityViewSchema, scope)
        # Assuming row version/id values stay the same for the before-update
        # rows, we can carry over values from the old view.
        oldIndices = self._index
        oldColumns = self.view.columns
        newView = utils.synread(self.syn, self._entityViewSchema.id, silent=True)
        for c in oldColumns:
            newView.loc[oldIndices,c] = self.view[c].values
        self.view = newView
        self._index = self.view.index


    def addActiveCols(self, activeCols, path=False, isMeta=False, backup=True):
        """ Add column names to `self._activeCols` or `self._metaActiveCols`.

        Parameters
        ----------
        activeCols : str, list, dict, or DataFrame
            Active columns to add.
        path : bool
            Optional. Whether the str passed in `activeCols` is a filepath.
            Defaults to False.
        meta : bool
            Optional. Whether we are adding active columns to the data or
            the metadata. Defaults to False.
        """
        if backup:
            self.backup("addActiveCols")
        # activeCols can be a str, list, dict, or DataFrame
        if isinstance(activeCols, str) and not path:
            if isMeta and activeCols not in self._metaActiveCols:
                self._metaActiveCols.append(activeCols)
            elif activeCols not in self._activeCols:
                self._activeCols.append(activeCols)
        elif isinstance(activeCols, (list, dict)) and not path:
            if isMeta:
                for c in activeCols:
                    if c not in self._metaActiveCols:
                        self._metaActiveCols.append(c)
            else:
                for c in activeCols:
                    if c not in self._activeCols:
                        self._activeCols.append(c)
        elif isinstance(activeCols, pd.DataFrame):
            # assumes column names are in first column
            for c in activeCols[activeCols.columns[0]]:
                if isMeta and c not in self._metaActiveCols:
                    self._metaActiveCols.append(c)
                elif c not in self._activeCols:
                    self._activeCols.append(c)
        elif path:
            pass

    def addDefaultValues(self, colVals, backup=True):
        """ Set all values in a column of `self.view` to a single value.

        Parameters
        ----------
        colVals : dict
            A mapping from column names to values
        backup : bool
            Optional. Whether to save the state of `self` before updating
            column values. Defaults to True.
        """
        if self.view is None:
            raise AttributeError("No data view set.")
        if backup:
            self.backup("addDefaultValues")
        for k in colVals:
            self.view[k] = colVals[k]

    def addKeyCol(self):
        """ Add a key column to `self.view`.

        A key column is a column in `self.view` whose values can be matched in a
        one-to-one manner with the column values of a column in `self._meta`.
        Usually this involves applying a regular expression to one of the
        columns in `self.view`. After a regular expression which satisfies the
        users requirements is found, the key column is automatically added to
        `self.view` with the same name as the column matched upon
        in `self._meta`.
        """
        if self.view is None or self._meta is None:
            raise AttributeError("No data view set.")
        self.backup("addKeyCol")
        link = self._linkCols(1)
        dataKey, metaKey = link.popitem()
        regex = ''
        print("Data", "\n\n")
        print("head")
        print(self.view[dataKey].head(), "\n\n")
        print()
        print("tail")
        print(self.view[dataKey].tail(), "\n\n")
        print("Metadata", "\n\n")
        print(self._meta[metaKey].head(), "\n\n")
        while True:
            regex = self._inputDefault("regex: ", regex)
            newCol = utils.colFromRegex(self.view[dataKey].values, regex)
            missingVals = [v not in self._meta[metaKey].values.astype(str)
                           for v in newCol]
            if any(missingVals):
                before_regex = self.view[dataKey][missingVals]
                after_regex = [newCol[i] for i in range(len(newCol))
                               if missingVals[i]]
                print("The following values were not found in the metadata:")
                for i in range(len(before_regex)):
                    print(after_regex[i], "<-", before_regex[i])
                print()
                proceedAnyways = self._getUserConfirmation()
                if proceedAnyways:
                    break
                else:
                    continue
            else:
                break
        self.keyCol = metaKey
        self.view[metaKey] = newCol

    def _inputDefault(self, prompt, prefill=''):
        """ Get input from the user from a prompt with preexisting text.

        Parameters
        ----------
        prompt : str
            Prompt to user.
        prefill : str
            Preexisting text to fill input with.

        Returns
        -------
        User input.
        """
        readline.set_startup_hook(lambda: readline.insert_text(prefill))
        try:
            return input(prompt)
        finally:
            readline.set_startup_hook()

    def addFileFormatCol(self, referenceCol='name', newColName='fileFormat'):
        """ Add a file format column using a preprogrammed regular expression.

        Parameters
        ----------
        referenceCol : str
            Optional. Column to parse file extension from. Defaults to 'name'.
        fileFormatColName : str
            Optional. Name of newly created column. Defaults to 'fileFormat'.
        """
        self.backup("addFileFormatCol")
        regex = r"\.(\w+)(?:\.gz)?$"
        filetypeCol = utils.colFromRegex(
                self.view[referenceCol].values, regex)
        self.view[newColName] = filetypeCol

    def addLinks(self, links=None, append=True, backup=True):
        """ Add link values to `self.links`

        Parameters
        ----------
        links : dict, optional
            Mappings from data columns to metadata columns to add
            to `self.links`. Calls `self._linkCols` if not set.

        Returns
        -------
        Links stored in `self.links`.
        """
        if self.view is None or self._meta is None:
            raise AttributeError("No data view set.")
        if backup:
            self.backup("addLinks")
        if links is None:
            links = self._linkCols(-1)
        if not isinstance(links, dict):
            raise TypeError("`links` must be a dictionary-like object")
        if not self.links or not append:
            self.links = links
        else:
            for k in links:
                self.links[k] = links[k]
        for k, v in links.items():
            if k not in self._activeCols:
                self._activeCols.append(k)
            if v not in self._metaActiveCols:
                self._metaActiveCols.append(v)
        return self.links

    def isValidKeyPair(self, dataCol=None, metaCol=None):
        """ Check if two columns are compatible to join upon.

        Parameters
        ----------
        dataCol : str, optional
            Column in `self.view`.
        metaCol : str, optional
            Column in `self._meta`.

        Returns
        -------
        Boolean, True if all dataCols are present in metaCol, False otherwise.
        """
        if dataCol is None and metaCol is None:
            dataCol, metaCol = self._linkCols(1).popitem()
        missingVals = set(self.view[dataCol]).difference(self._meta[metaCol])
        if missingVals:
            print("The following values are missing:", end="\n")
            for v in missingVals:
                print(v)
            return False
        return True

    def substituteColumnValues(self, col, mod):
        """ Substitute values in a column according to a mapping.

        Parameters
        ----------
        col : str
            The column to substitute values in.
        mod : dict
            Mappings from the old to new values.
        """
        self.backup("substituteColumnValues")
        self.view.loc[:, col] = utils.substituteColumnValues(
                self.view[col].values)

    def _parseView(self, view, sortCols, isMeta=False):
        """ Turn `view` into a pandas DataFrame.

        Parameters
        ----------
        view : str, list, or pandas.DataFrame
            `list` is only supported if `isMeta` is True.
        sortCols : bool
            whether to order columns lexicographically in the returned DataFrame.

        Returns
        -------
        A pandas.DataFrame

        Raises
        ------
        TypeError if view is not a str, list, or pandas.DataFrame
        """
        if isinstance(view, str):
            return utils.synread(self.syn, view, sortCols=sortCols)
        elif isinstance(view, list) and meta:
            return utils.combineSynapseTabulars(self.syn, view, axis=1)
        elif isinstance(view, pd.DataFrame):
            if sortCols:
                view = view.sort_index(1)
            return deepcopy(view)
        else:
            raise TypeError(
                    "{} is not a supported data input type".format(type(view)))

    def publish(self, validate=True):
        """ Store `self.view` back to the file view it was derived
        from on Synapse.

        Parameters
        ----------
        validate : bool
            Optional. Whether to warn of possible errors in `self.view`.
            Defaults to True.
        """
        if validate:
            warnings = self._validate()
            if len(warnings):
                for w in warnings:
                    print(w)
                print()
                continueAnyways = self._getUserConfirmation()
                if not continueAnyways:
                    print("Publish canceled.")
                    return
        t = sc.Table(self._entityViewSchema.id, self.view)
        print("Storing to Synapse...")
        t_online = self.syn.store(t)
        print("Fetching new table index...")
        self.view = utils.synread(self.syn, self._entityViewSchema.id)
        self._index = self.view.index
        print("You're good to go :~)")
        return self._entityViewSchema.id

    def _getUserConfirmation(self, message="Proceed anyways? (y) or (n): "):
        """ Get confirmation from user.

        Parameters
        ----------
        message : str
            Message to print when asking for confirmation.

        Returns
        -------
        True if user input begins with 'Y' or 'y'.
        False if user input begins with 'N' or 'n'.
        Otherwise asks user to input confirmation again.
        """
        proceed = ''
        while not proceed:
            proceed = input(message)
            if len(proceed) and not proceed[0] in ['Y', 'y', 'N', 'n']:
                proceed = ''
                print("Please enter 'y' or 'n': ", end='')
            elif len(proceed) and proceed[0].lower() == 'y':
                return True
            elif len(proceed) and proceed[0].lower() == 'n':
                return False

    def onweb(self):
        """ View the file view which `self.view` derives from in a browser. """
        self.syn.onweb(self._entityViewSchema.id)

    def _validate(self):
        """ Validate `self.view` before publishing to warn of possible errors.

        Currently only checks if any active columns have any null values.
        """
        warnings = []
        # check that no columns have null values
        null_cols = self.view[self._activeCols].isnull().any()
        for i in null_cols.iteritems():
            col, hasna = i
            if hasna:
                warnings.append("{} has null values.".format(col))
        # cross check values with allowed values in self.schema
        if self.schema is not None:
            malformed_values = schemaModule.validateView(self.view, self.schema)
            if malformed_values:
                for k in malformed_values:
                    warnings.append("{} contains the following values which are "
                                    "not specified in the schema: {}".format(
                                        k, ", ".join(map(str, malformed_values[k]))) +
                                    "\n\tPossible values are {}".format(
                                        ", ".join(self.schema.loc[k].value.values)))
        return warnings

    def removeActiveCols(self, activeCols):
        """ Remove a column name from `self._activeCols`

        Parameters
        ----------
        activeCols : str or list-like
            Column name(s) to remove.
        """
        self.backup("removeActiveCols")
        if isinstance(activeCols, str):
            self._activeCols.remove(activeCols)
        else:  # is list-like
            for c in activeCols:
                self._activeCols.remove(c)

    def _getUniqueCols(self, newCols, preexistingCols):
        """ Replace `preexisingCols` with `newCols`.

        Parameters
        ----------
        newCols : list of dict-like
            New columns to replace columns in `preexisingCols`.
        preexisingCols : list of dict-like
            Old columns to be replaced by `newCols` (if a replacement
            is present).
        """
        preexistingColNames = [c['name'] for c in preexistingCols]
        uniqueCols = []
        for c in newCols:
            if c['name'] in preexistingColNames:
                # default behavior is to replace the older column with the newer
                isCol = [c_['name'] == c['name'] for c_ in preexistingCols]
                preexistingCols.pop(isCol.index(True))
            uniqueCols.append(c)
        uniqueCols += preexistingCols
        return uniqueCols

    def valueCounts(self):
        """ Print the value counts of all `self._activeCols`. """
        for c in self._activeCols:
            print(self.view[c].value_counts(dropna=False), end="\n\n")

    def _prettyPrintColumns(self, cols, style):
        """ Helper function to print columns in a legible way.

        Parameters
        ----------
        cols : list-like
            List of strings to print.
        style : str
            One of 'letters' or 'numbers'.
        """
        if style == 'letters':
            for i in range(len(cols)):
                padding = " " if (len(cols) > 26 and (65 + i <= 90)) else ""
                if 65 + i > 90:
                    i_ = i % 26
                    print("A{}".format(chr(65 + i_)), "{}|".format(padding),
                          cols[i])
                else:
                    print(chr(65 + i), "{}|".format(padding), cols[i])
        elif style == 'numbers':
            for i in range(len(cols)):
                padding = " " if (len(cols) > 10 and i < 10) else ""
                print(str(i), "{}|".format(padding), cols[i])

    def createFileView(self, name, parent, scope, addCols=None, schema=None):
        """ Create and store a file view for further manipulation.

        Parameters
        ----------
        name : str
            The name of the file view.
        parent : str
            Synapse ID of project to store file view within.
        scope : str or list
            Synapse IDs of items to include in file view.
        addCols : dict, list, or str
            Columns to add in addition to the default file view columns.
        schema : str or pandas.DataFrame
            A path to a .json file specifying a schema the file view should
            conform to -- or a pandas.DataFrame alreay in flattened format.
            (See `schema.flattenJson`).

        If `addCols` is a dict:
            Add keys as columns. If a key's value is `None`, then insert an empty
            column. Otherwise, set the `defaultValue` of the column to that value.
            After setting `self.view` to the pandas DataFrame version of the newly
            created file view, all rows in each column will be set to its
            `defaultValue` (unless there is no `defaultValue`, in which case the
            column will be empty). The file view will not be updated on Synapse
            until `self.publish` is called.
        If `addCols` is a list:
            Add columns to schema with no `defaultValue`. `self.view` will be
            unchanged from the file view that is stored on Synapse.
        If 'addCols is a str:
            Assumes the string is a filepath. Attempts to read in the filepath as
            a two-column .csv file, and then proceeds as if `addCols` was a dict,
            where the first column are the keys and the second column are the
            values.

        Returns
        -------
        Synapse ID of newly created fileview.
        """
        self.backup("createFileView")

        # Fetch default keys, plus any preexisting annotation keys
        cols = utils.getDefaultColumnsForScope(self.syn, scope)

        # Store flattened schema, add keys to active columns list.
        if self.schema is None:
            self.schema = (
                    schemaModule.flattenJson(schema) if isinstance(schema, str)
                    else schema)
        if self.schema is not None:
            for k in self.schema.index.unique():
                self.addActiveCols(k)
            schemaCols = utils.makeColumns(list(self.schema.index.unique()),
                                           asSynapseCols=False)
            cols = self._getUniqueCols(schemaCols, cols)

        # Add keys defined during initialization
        if self._activeCols:
            activeCols = utils.makeColumns(self._activeCols,
                                           asSynapseCols=False)
            cols = self._getUniqueCols(activeCols, cols)

        # Add keys passed to addCols
        if addCols:
            if isinstance(addCols, dict):
                unspecifiedCols = [k for k in addCols if addCols[k] is None]
                self.addActiveCols(unspecifiedCols)
            elif isinstance(addCols, list):
                self.addActiveCols(addCols)
            newCols = utils.makeColumns(addCols, asSynapseCols=False)
            cols = self._getUniqueCols(newCols, cols)

        # Store columns to Synapse as EntityViewSchema. Default column values
        # are added to `self.view` but not yet stored to Synapse.
        cols = [sc.Column(**c) for c in cols]
        entityViewSchema = sc.EntityViewSchema(name=name, columns=cols,
                                               parent=parent, scopes=scope)
        self._entityViewSchema = self.syn.store(entityViewSchema)
        self.view = utils.synread(self.syn, self._entityViewSchema.id)
        self._index = self.view.index
        if isinstance(addCols, dict):
            self.addDefaultValues(addCols, False)
        return self._entityViewSchema.id

    def transferLinks(self, cols=None, on=None, how='left', dropOn=True):
        """ Copy metadata to `self.view`, matching on `self.keyCol`.

        Parameters
        ----------
        cols : list-like
            Optional. A subset of columns which have been linked
            with `self.addLinks` to transfer metadata values to.
            Defaults to all linked columns.
        on : str, optional
            Column to match data with metadata.
            Defaults to `self.keyCol`.
        how : str, optional
            How to merge the metadata on the data.
            Defaults to 'left' (keep only the keys in the data).
        dropOn : bool, optional
            Drops the column, `on`, used to align the data
            with the metadata. Defaults to True.

        After adding a key column (`self.addKeyCol`) and linking the data
        columns to the metadata columns (`self.addLinks`), transfer the
        values from the metadata to the data, aligning on `on`.
        """
        if on is None:
            on = self.keyCol
        if not self.links:
            raise RuntimeError("Need to link metadata values first.")
        self.backup("transferLinks")
        if not cols:
            cols = list(self.links.keys())
            if on in cols:
                cols.pop(cols.index(on))
        metaCols = list(set(self.links.values()))
        renamedCols = {}
        for c in metaCols:
            if c in self.view.columns:
                renamedCols[c] = "{}_meta".format(c)
        metaCols.append(on)
        relevant_meta = self._meta.loc[:, metaCols]
        relevant_meta.rename(columns=renamedCols, inplace=True)
        # prevent type comparison errors
        relevant_meta[on] = relevant_meta[on].astype(str)
        self.view[on] = self.view[on].astype(str)
        merged = self.view.merge(relevant_meta, on=on, how=how)
        # if there are duplicates in the data this may break things
        merged.drop_duplicates(inplace=True)
        print("original", self.view.shape)
        print("merged", merged.shape)
        for c in cols:
            v = self.links[c]
            if v in renamedCols:
                v = renamedCols[c]
            self.view[c] = merged[v].values
        if dropOn:
            self.view.drop(on, 1, inplace=True)

    def inferValues(self, col, referenceCols):
        """ Fill in values for indices which match on `referenceCols`
        and which have a single, unique, non-NaN value in `col`.

        Parameters
        ----------
        col : str
            Column to fill with values.
        referenceCols : list or str
            Column(s) to match on.
        """
        if self.view is None:
            raise AttributeError("No data view set.")
        self.backup("inferValues")
        self.view = utils.inferValues(self.view, col, referenceCols)

    def _linkCols(self, iters):
        """ Helper function to return a dictionary with data columns as keys
        and metadata columns as values. Useful when drawing links between
        the data and the metadata.

        Parameters
        ----------
        iters : int
            Number of iterations before returning a dictionary. If set to a
            negative number, will iterate until a newline is entered as input.

        Returns
        -------
        Dictionary containing select data column names as keys and metadata
            column names as values.
        """
        links = {}

        def _verifyInputIntegrity(i, view):
            if i is '':
                return -1
            try:
                i = int(i)
                assert i < len(view.columns) and i >= 0
            except:
                print("Please enter an integer corresponding to "
                      "one of the columns above.", "\n")
                return
            return i

        while iters != 0:
            print("Data:", "\n")
            self.columns("numbers")
            print()
            data_col = None
            while data_col is None:
                data_col = input("Select a data column: ")
                data_col = _verifyInputIntegrity(data_col, self.view)
            if data_col == -1:
                return links
            print("\n", "Metadata", "\n")
            self.metaColumns("numbers")
            print()
            metadata_col = None
            while metadata_col is None:
                metadata_col = input("Select a metadata column: ")
                metadata_col = _verifyInputIntegrity(metadata_col, self._meta)
            if metadata_col == -1:
                return links
            data_val = self.view.columns[data_col]
            metadata_val = self._meta.columns[metadata_col]
            links[data_val] = metadata_val
            iters -= 1
        return links
