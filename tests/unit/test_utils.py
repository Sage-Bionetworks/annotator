import pytest
import annotator
import synapseclient
import pandas
import os
import uuid
import tempfile
from . import conftest


class TestSynread(object):
    def test_synread_csv(self, syn, sampleFile, entities):
        df = annotator.utils.synread(syn, entities['files'][0].id,
                                     sortCols=False)
        pandas.testing.assert_frame_equal(df, sampleFile, check_like=True)

    def test_synread_table(self, syn, sampleFile, entities):
        df = annotator.utils.synread(syn, entities['table_schema'].id,
                                     sortCols=False)
        pandas.testing.assert_frame_equal(
                df.reset_index(drop=True),
                sampleFile,
                check_like=True)

    def test_synread_entity_view(self, syn, entities):
        df = annotator.utils.synread(
                syn,
                entities['entity_view'].id,
                sortCols=False)
        assert isinstance(df, pandas.DataFrame)

    def test_synread_list_csv(self, syn, sampleFile, entities):
        listOfDataFrames = annotator.utils.synread(
                syn,
                [f['id'] for f in entities['files']],
                sortCols=False)
        pandas.testing.assert_frame_equal(
                listOfDataFrames[0],
                sampleFile,
                check_like=True)


class TestSynapseColumnCreation(object):
    @pytest.fixture
    def keys_and_vals(self):
        keys = ['hello', 'goodbye']
        values = ['world', '']
        correctResult = [
                {'name': 'hello', 'maximumSize': 5,
                 'columnType': 'STRING', 'defaultValue': 'world'},
                {'name': 'goodbye', 'maximumSize': 50,
                 'columnType': 'STRING', 'defaultValue': ''}]
        return keys, values, correctResult

    def test__keyValCols_not_Synapse_cols(self, keys_and_vals):
        keys, values, correctResult = keys_and_vals
        result = annotator.utils._keyValCols(keys, values, False)
        assert result == correctResult

    def test__keyValCols_as_Synapse_cols(self, keys_and_vals):
        keys, values, correctResult = keys_and_vals
        result = annotator.utils._keyValCols(keys, values, True)
        assert all([isinstance(i, synapseclient.Column) for i in result])

    def test__colsFromFile(self, keys_and_vals):
        keys, values, correctResult = keys_and_vals
        path = tempfile.mkstemp()[1]
        df = pandas.DataFrame(list(zip(keys, values)))
        df.to_csv(path, index=False, header=False)
        result = annotator.utils._colsFromFile(path, False)
        assert result == correctResult

    def test__colsFromDict(self, keys_and_vals):
        keys, values, correctResult = keys_and_vals
        dictionary = {k: v for k, v in zip(keys, values)}
        result = annotator.utils._colsFromDict(dictionary, False)
        assert result == correctResult

    def test__colsFromList(self, keys_and_vals):
        keys, values, correctResult = keys_and_vals
        result = annotator.utils._colsFromList(keys, False)
        for c in correctResult:
            c['maximumSize'] = 50
            c['defaultValue'] = ''
        assert result == correctResult

    def test_makeColumns(self):
        with pytest.raises(TypeError):
            annotator.utils.makeColumns(float("nan"))


class TestColumnModification(object):
    def test_dropColumns_str(self, syn, genericEntityView):
        schema = annotator.utils.dropColumns(syn, genericEntityView.id,
                                             cols='etag')
        new_view = syn.tableQuery("select * from {}".format(schema.id))
        new_view = new_view.asDataFrame()
        assert 'etag' not in new_view.columns

    def test_dropColumns_list(self, syn, genericEntityView):
        schema = annotator.utils.dropColumns(syn, genericEntityView.id,
                                             cols=['type', 'createdOn'])
        new_view = syn.tableQuery("select * from {}".format(schema.id))
        new_view = new_view.asDataFrame()
        assert 'type' not in new_view.columns \
            and 'createdOn' not in new_view.columns


class TestScopeModification(object):
    @pytest.fixture(scope='class')
    def scopeFolders(self, syn, project):
        folder_one = conftest.folder(syn, project)
        folder_two = conftest.folder(syn, project)
        folder_three = conftest.folder(syn, project)
        file_one = conftest.file_(syn, folder_one,
                                  annotations={'color': 'red'})
        file_two = conftest.file_(syn, folder_two,
                                  annotations={'pizza': 'pineapple'})
        file_three = conftest.file_(syn, folder_three,
                                    annotations={'cookie': 'monster'})
        return {1: folder_one, 2: folder_two, 3: folder_three}

    """
    # TODO wait for POST call issue to be resolved
    def test_getDefaultColumnsForScope(self, entities):
        result = annotator.getDefaultColumnsForScope(entities['entity_view'])
        result_names = {c['name'] for c in result}
        correctResult = {'concreteType'}
    """

    def test_addToScope_singular(self, syn, project, scopeFolders):
        entity_view = conftest.entity_view(syn, project, scopeFolders[1].id)
        schema = annotator.utils.addToScope(
                syn,
                entity_view,
                scopeFolders[2].id)
        q = syn.tableQuery("select * from {}".format(schema.id))
        df = q.asDataFrame()
        assert 'pizza' in df.columns
        assert len(df) == 2

    def test_addToScope_multiple(self, syn, project, scopeFolders):
        entity_view = conftest.entity_view(syn, project, scopeFolders[1].id)
        schema = annotator.utils.addToScope(
                syn,
                entity_view,
                [scopeFolders[2].id, scopeFolders[3].id])
        q = syn.tableQuery("select * from {}".format(schema.id))
        df = q.asDataFrame()
        assert 'pizza' in df.columns
        assert 'cookie' in df.columns
        assert len(df) == 3


class TestMisc(object):
    def test_combineSynapseTabulars_axis_zero(self, syn, entities):
        result = annotator.utils.combineSynapseTabulars(
                syn=syn,
                tabulars=[f['id'] for f in entities['files']],
                axis=1)
        assert result.shape == (2, 6)

    def test_combineSynapseTabulars_axis_one(self, syn, entities):
        result = annotator.utils.combineSynapseTabulars(
                syn=syn,
                tabulars=[f['id'] for f in entities['files']],
                axis=0)
        assert result.shape == (6, 2)

    def test_compareDicts(self):
        d1 = {'one': 'pizza', 'two': 'pizza', 'three': 'pizza'}
        d2 = {'four': 'cookie', 'three': 'cookie', 'two': 'pizza'}
        result = annotator.utils.compareDicts(d1, d2)
        assert result == ({'one'}, {'four'}, {'three'})

    def test_clipboardToDict(self):
        string = "hello:world\ngoodbye:moon"
        os.system("echo '{}' | pbcopy".format(string))
        result = annotator.utils.clipboardToDict(":")
        assert result == {'hello': 'world', 'goodbye': 'moon'}


class TestValueCreation(object):
    @pytest.fixture(scope='class')
    def metaTable(self, syn, project, sampleMetadata):
        return conftest.table(syn, project, sampleMetadata)

    def test_inferValues(self, syn, metaTable):
        q = syn.tableQuery("select * from {}".format(metaTable.id))
        df = q.asDataFrame()
        print(df)
        df = df.set_index(keys='id', drop=True)
        result_one = annotator.utils.inferValues(
                df=df,
                col='favoriteMeat',
                referenceCols='team')
        result_two = annotator.utils.inferValues(
                df=df,
                col='favoriteFruit',
                referenceCols='team')
        print(type(result_two.loc[3, 'favoriteFruit']),
              result_two.loc[3, 'favoriteFruit'])
        assert result_one.loc[3, 'favoriteMeat'] == 'bacon'
        assert pandas.isnull(result_two.loc[3, 'favoriteFruit'])


class TestValueModification(object):
    @pytest.fixture(scope='class')
    def values(self):
        return ['blue', 'blue', 'red', 'green', 'potato']

    def test_substituteColumnValues(self, values):
        result = annotator.utils.substituteColumnValues(
                values, {'blue': 'red', 'potato': 'waffle'})
        assert result == ['red', 'red', 'red', 'green', 'waffle']

    def test_colFromRegex(self, values):
        # gives a list of the first vowel of each word
        result = annotator.utils.colFromRegex(values, r"([aeiou])")
        assert result == ['u', 'u', 'e', 'e', 'o']
