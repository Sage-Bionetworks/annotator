import pytest
import annotator
import synapseclient
import pandas
import os

def test_setUp(syn, sampleEntities, entities):
    """ Set up working environment on Synapse,
    passes if set-up is successful
    """
    assert True


def test_synread_csv(syn, sampleEntities, entities):
    df = annotator.utils.synread(syn, entities['files'][0].id, sortCols=False)
    pandas.testing.assert_frame_equal(df, sampleEntities['data'],
                                      check_like=True)


def test_synread_table(syn, sampleEntities, entities):
    df = annotator.utils.synread(syn, entities['schema'].id, sortCols=False)
    pandas.testing.assert_frame_equal(
            df.reset_index(drop=True),
            sampleEntities['data'],
            check_like=True)


def test_synread_entity_view(syn, sampleEntities, entities):
    df = annotator.utils.synread(
            syn,
            entities['entity_view'].id,
            sortCols=False)
    assert isinstance(df, pandas.DataFrame)


def test_synread_list_csv(syn, sampleEntities, entities):
    listOfDataFrames = annotator.utils.synread(
            syn,
            [f['id'] for f in entities['files']],
            sortCols=False)
    pandas.testing.assert_frame_equal(
            listOfDataFrames[0],
            sampleEntities['data'],
            check_like=True)

def test_clipboardToDict():
    string = "hello:world\ngoodbye:moon"
    os.system("echo '{}' | pbcopy".format(string))
    result = annotator.utils.clipboardToDict(":")
    assert result == {'hello':'world', 'goodbye':'moon'}


class TestColumnCreation(object):
    @pytest.fixture
    def keys_and_vals(self):
        keys = ['hello', 'goodbye']
        values = ['world', '']
        return keys, values

    def test__keyValCols_not_Synapse_cols(self, keys_and_vals):
        keys, values = keys_and_vals
        result = annotator.utils._keyValCols(keys, values, False)
        correctResult = [
                {'name': 'hello', 'maximumSize': 5,
                 'columnType': 'STRING', 'defaultValue': 'world'},
                {'name': 'goodbye', 'maximumSize': 50,
                 'columnType': 'STRING', 'defaultValue': ''}]
        assert result == correctResult

    def test__keyValCols_as_Synapse_cols(self, keys_and_vals):
        keys, values = keys_and_vals
        result = annotator.utils._keyValCols(keys, values, True)
        assert all([isinstance(i, synapseclient.Column) for i in result])
