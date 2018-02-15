import annotator
import pytest
import pandas
from . import conftest


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
        monkeypatch.setattr('builtins.input',
                            lambda _: next((i for i in inputs)))
        for i in inputs:
            assert genericPipeline._getUserConfirmation()

    def test_getUserConfirmation_no(self, genericPipeline, monkeypatch):
        inputs = ['no', 'n', 'NO thank you']
        monkeypatch.setattr('builtins.input',
                            lambda _: next((i for i in inputs)))
        for i in inputs:
            assert not genericPipeline._getUserConfirmation()

    def test_getUserConfirmation_unknown(self, genericPipeline, monkeypatch):
        inputs = ['wut', '#$%!#$%', 'Show me what u got']
        monkeypatch.setattr('builtins.input',
                            lambda _: next((i for i in inputs)))
        for i in inputs:
            assert not genericPipeline._getUserConfirmation()


class TestActiveColumns(object):
    @pytest.fixture
    def pipeline(self, genericPipeline):
        genericPipeline._activeCols = ["pizza", "pie"]
        genericPipeline._metaActiveCols = ["pizza", "pie"]
        return genericPipeline

    def test_removeActiveCols_str(self, pipeline):
        pipeline.removeActiveCols("pizza", backup=False)
        assert pipeline._activeCols == ["pie"]

    def test_removeActiveCols_list(self, pipeline):
        pipeline.removeActiveCols(["pizza", "pie"], backup=False)
        assert pipeline._activeCols == []

    def test_addActiveCols_str(self, pipeline):
        pipeline.addActiveCols("marmalade", backup=False)
        assert "marmalade" in pipeline._activeCols

    def test_addActiveCols_list(self, pipeline):
        pipeline.addActiveCols(["sunday", "funday"], backup=False)
        assert pipeline._activeCols == ["pizza", "pie", "sunday", "funday"]

    def test_addActiveCols_dict(self, pipeline):
        pipeline.addActiveCols(
                {"creme": "brulee", "chai": "latte"}, backup=False)
        assert "creme" in pipeline._activeCols
        assert "chai" in pipeline._activeCols

    def test_addActiveCols_dataframe(self, pipeline):
        df = pandas.DataFrame({"key": ["chai", "creme"],
                               "value": ["latte", "brulee"]})
        pipeline.addActiveCols(df, backup=False)
        assert "creme" in pipeline._activeCols
        assert "chai" in pipeline._activeCols

    def test_addActiveCols_str_meta(self, pipeline):
        pipeline.addActiveCols("marmalade", isMeta=True, backup=False)
        assert "marmalade" in pipeline._metaActiveCols

    def test_addActiveCols_list_meta(self, pipeline):
        pipeline.addActiveCols(["sunday", "funday"], isMeta=True, backup=False)
        assert pipeline._metaActiveCols == ["pizza", "pie", "sunday", "funday"]

    def test_addActiveCols_dict_meta(self, pipeline):
        pipeline.addActiveCols({"creme": "brulee", "chai": "latte"},
                               isMeta=True, backup=False)
        assert "creme" in pipeline._metaActiveCols
        assert "chai" in pipeline._metaActiveCols

    def test_addActiveCols_dataframe_meta(self, pipeline):
        df = pandas.DataFrame(
                {"key": ["chai", "creme"], "value": ["latte", "brulee"]})
        pipeline.addActiveCols(df, isMeta=True, backup=False)
        assert "creme" in pipeline._metaActiveCols
        assert "chai" in pipeline._metaActiveCols

    def test_removeActiveCols_str(self, pipeline):
        pipeline.removeActiveCols("pizza", backup=False)
        assert "pizza" not in pipeline._activeCols

    def test_removeActiveCols_list(self, pipeline):
        pipeline.removeActiveCols(["pizza", "pie"], backup=False)
        assert ("pizza" not in pipeline._activeCols and
                "pie" not in pipeline._activeCols)


class TestScopeModification(object):
    def test_addView(self, syn, entities, project):
        entity_view = conftest.entity_view(
                syn, project, entities["folders"][0])
        p = annotator.Pipeline(syn, view=entity_view.id)
        p.addView(entities["folders"][1].id)
        correctScopeIds = [f.id[3:] for f in entities["folders"]]
        assert all([i in correctScopeIds
                    for i in p._entityViewSchema["scopeIds"]])


class TestDefaultValues(object):
    def test_addDefaultValues_preexisting_col(self, genericPipeline, sampleFile):
        genericPipeline.view = sampleFile
        genericPipeline.addDefaultValues(
                {"favoriteColor": "purple"}, backup=False)
        assert all([v == "purple"
                    for v in genericPipeline.view["favoriteColor"]])

    def test_addDefaultValues_new_col(self, genericPipeline, sampleFile):
        genericPipeline.view = sampleFile
        genericPipeline.addDefaultValues(
                {"favoriteCheese": "Stinky Bishop"}, backup=False)
        assert all([v == "Stinky Bishop"
                    for v in genericPipeline.view["favoriteCheese"]])


class TestFileFormatColumn(object):
    @pytest.fixture
    def sampleView(self):
        return pandas.DataFrame(
                {"name": ["celery_man.gif", "fastq.fastq", "unknown",
                          "slim.fastq.gz", "slim.tar.gz", None]})

    def test_addFileFormatCol(self, genericPipeline, sampleView):
        genericPipeline.view = sampleView
        genericPipeline.addFileFormatCol()
        assert all([i == j for i, j in
                    zip(genericPipeline.view["fileFormat"],
                        ["gif", "fastq", None, "fastq", "tar", None])])


class TestLinks(object):
    @pytest.fixture
    def pipeline(self, syn, genericPipeline, entities, sampleMetadata):
        view = syn.tableQuery("select * from {}".format(
            entities['entity_view'].id)).asDataFrame()
        genericPipeline.view = view
        genericPipeline._meta = sampleMetadata
        return genericPipeline

    def test_addLinks(self, pipeline):
        links = {'name': 'mexico', 'createdBy': 'serbia'}
        pipeline.addLinks(links=links, append=False, backup=False)
        assert pipeline.links == links
        links2 = {'name': 'mexico'}
        pipeline.addLinks(links=links2, append=False, backup=False)
        assert pipeline.links == links2
        links3 = {'createdBy': 'serbia'}
        pipeline.addLinks(links=links3, append=True, backup=False)
        assert pipeline.links == links
        with pytest.raises(TypeError):
            pipeline.addLinks(links="hello", append=False, backup=False)
        with pytest.raises(AttributeError):
            pipeline.view = None
            pipeline.addLinks()

    def test_transferLinks(self, pipeline):
        pipeline.view['id'] = [1,2,3]
        pipeline.view['spanishWords'] = None
        pipeline.view['serbianWords'] = None
        pipeline.keyCol = 'id'
        pipeline.links = {'spanishWords': 'mexico', 'serbianWords': 'serbia'}
        pipeline.transferLinks()
        assert all(pipeline.view['spanishWords'] == ['quien', 'que', 'donde'])
        assert all(pipeline.view['serbianWords'] == ['ко', 'Шта', 'где'])
        assert 'id' not in pipeline.view # by default we drop the `on` column


class TestKey(object):
    @pytest.fixture(scope='class')
    def pipeline(self, genericPipeline, sampleFile, sampleMetadata):
        genericPipeline.view = sampleFile
        genericPipeline._meta = sampleMetadata
        return genericPipeline

    def test_isValidKeyPair(self, pipeline):
        # no overlap
        assert not pipeline.isValidKeyPair("name", "mexico")
        # complete overlap (view is subset of meta)
        pipeline.view["mexico_view"] = list(
                pipeline._meta["mexico"][:len(pipeline.view)])
        assert pipeline.isValidKeyPair("mexico_view", "mexico")
        # partial overlap
        pipeline.view.loc[0, 'mexico_view'] = None
        assert not pipeline.isValidKeyPair("mexico_view", "mexico")


class TestMisc(object):
    @pytest.fixture(scope='class')
    def pipeline(self, genericPipeline, sampleFile):
        genericPipeline.view = sampleFile
        return genericPipeline

    def test_substituteColumnValues(self, pipeline):
        pipeline.substituteColumnValues(
                "name", {"phil": "Phillip", "tom": "Tomothy"})
        assert pipeline.view["name"].values == ["Phillip", "Tomothy"]


class TestPublish(object):
    @pytest.fixture(scope='class')
    def pipeline(self, genericPipeline, sampleFile):
        genericPipeline.view = sampleFile
        return genericPipeline

    def test_publish_no_view_or_schema(self, pipeline, genericPipeline):
        with pytest.raises(AttributeError):
            # no view or schema set
            genericPipeline.publish(validate=False)
        with pytest.raises(AttributeError):
            # view set but no schema set
            pipeline.publish(validate=False)

    def test_publish(self, syn, entities, genericPipeline):
        q = syn.tableQuery("select * from {}".format(
            entities['entity_view'].id))
        genericPipeline.view = q.asDataFrame()
        genericPipeline._entityViewSchema = entities['entity_view']
        genericPipeline.view['type'] = 'updatedFile'
        genericPipeline.publish(validate=False)
        published_view = syn.tableQuery("select * from {}".format(
            entities['entity_view'].id)).asDataFrame()
        pandas.testing.assert_frame_equal(
                genericPipeline.view,
                published_view,
                check_like=True)


class TestCreateFileView(object):
    @pytest.fixture(scope='class')
    def pipeline(self, genericPipeline, entities):
        additionalCols = {"coffee": "black", "cheese": None}
        genericPipeline.createFileView(
                name="--Test--",
                parent=entities['project'].id,
                scope=entities['project'].id,
                addCols=additionalCols)
        return genericPipeline

    def test_createFileView(self, pipeline):
        published_view = syn.tableQuery("select * from {}".format(
            pipeline._entityViewSchema.id)).asDataFrame()
        # check default value propogated locally but not globally
        assert all([v == "black" for v in pipeline.view["coffee"]])
        assert all([pandas.isnull(v) for v in published_view["coffee"]])
        # check that columns with unspecified values were created
        assert "cheese" in pipeline.view
        assert "cheese" in published_view
        # check that preexisting annotations were included
        assert any([pd.notnull(v) for v in
                    pipeline.view['preexistingAnnotation']])
        assert any([pd.notnull(v) for v in
                    published_view['preexistingAnnotation']])
