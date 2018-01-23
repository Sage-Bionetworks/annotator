import annotator
import pytest
import pandas


class TestInitialization(object):
    def test_minimal(self, genericPipeline):
        assert isinstance(genericPipeline, annotator.Pipeline)

    def test_minimal_type_error(self):
        with pytest.raises(TypeError):
            p = annotator.Pipeline(0)


class TestViewParsing(object):
    def test__parseView_df(self, sampleFile, genericPipeline):
        view = genericPipeline._parseView(sampleFile, sortCols=False)
        pandas.testing.assert_frame_equal(view, sampleFile)

    def test__parseView_str(self, sampleFile, genericPipeline, entities):
        view = genericPipeline._parseView(entities['files'][0].id,
                                          sortCols=False)
        pandas.testing.assert_frame_equal(view, sampleFile)

    def test__parseView_list_meta(self, sampleFile, genericPipeline, entities):
        view = genericPipeline._parseView(
                [f['id'] for f in entities['files']],
                sortCols=False,
                isMeta=True)
        block_shape = sampleFile.shape
        assert view.shape == (
                block_shape[0],
                len(entities['files'])*block_shape[1])

    def test__parseView_list_not_meta(self, genericPipeline, entities):
        with pytest.raises(TypeError):
            view = genericPipeline._parseView(
                    [f['id'] for f in entities['files']],
                    sortCols=False,
                    isMeta=False)

    def test__parseView_sortCols(self, sampleFile, genericPipeline):
        view = genericPipeline._parseView(sampleFile, sortCols=True)
        assert isinstance(view, pandas.DataFrame)
        assert list(sorted(sampleFile.columns)) == list(view.columns)

class TestConfirmationPrompt(object):
    def test_getUserConfirmation_yes(self, genericPipeline, monkeypatch):
        inputs = ['yes', 'y', 'YeS']
        monkeypatch.setattr('builtins.input', lambda _: next((i for i in inputs)))
        for i in inputs:
            assert genericPipeline._getUserConfirmation()

    def test_getUserConfirmation_no(self, genericPipeline, monkeypatch):
        inputs = ['no', 'n', 'NO thank you']
        monkeypatch.setattr('builtins.input', lambda _: next((i for i in inputs)))
        for i in inputs:
            assert not genericPipeline._getUserConfirmation()

    def test_getUserConfirmation_unknown(self, genericPipeline, monkeypatch):
        inputs = ['wut', '#$%!#$%', 'Show me what u got']
        monkeypatch.setattr('builtins.input', lambda _: next((i for i in inputs)))
        for i in inputs:
            assert not genericPipeline._getUserConfirmation()
