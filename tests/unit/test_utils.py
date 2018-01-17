import annotator
import pandas


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
