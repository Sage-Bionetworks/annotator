from future.utils import iteritems
import pandas
import synapseclient
from annotator import schema


def test_annotation_key_lengths_json_vs_table():
    """
    For each annotation module, compares the length of annotation keys of json files on github with the current melted
    annotation synapse table keys generated by json2synapse.py
    """
    syn = synapseclient.login()

    tableSynId = "syn10242922"

    names = schema.moduleJsonPath()
    currentTable = syn.tableQuery("SELECT * FROM %s" % tableSynId)
    currentTable = currentTable.asDataFrame()

    for module, path in iteritems(names):
        table_key_set = set(currentTable[currentTable['module'] == module].key.unique())
        json_record = pandas.read_json(path)
        json_key_set = set(json_record['name'])

        assert len(json_key_set) == len(table_key_set)
