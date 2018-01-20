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


def test_clipboardToDict():
    string = "hello:world\ngoodbye:moon"
    os.system("echo '{}' | pbcopy".format(string))
    result = annotator.utils.clipboardToDict(":")
    assert result == {'hello': 'world', 'goodbye': 'moon'}


class TestColumnCreation(object):
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
    def test_addToScope(self, syn, project):
        folder_one = conftest.folder(syn, project)
        folder_two = conftest.folder(syn, project)
        file_one = conftest.file_(syn, folder_one, {'color': 'red'})
        file_two = conftest.file_(syn, folder_two, {'pizza': 'pineapple'})
        entity_view = conftest.entity_view(syn, project, folder_one.id)
        schema = annotator.utils.addToScope(syn, entity_view, folder_two.id)
        q = syn.tableQuery("select * from {}".format(schema.id))
        df = q.asDataFrame()
        assert 'pizza' in df.columns
        assert len(df) == 2
