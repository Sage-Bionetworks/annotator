import os
import requests
import json
import argparse
import pandas
import synapseclient
import schema


syn = synapseclient.login()


def updateTable(tableSynId, newTable, releaseVersion):
    """
    Gets the current annotation table, deletes all it's rows, then updates the table with new content generated
    from all the json files on synapseAnnotations. In the process also updates the table annotation to the latest release version.
    """

    currentTable = syn.tableQuery("SELECT * FROM %s" % tableSynId)

    # If current table has rows, delete all the rows
    if currentTable.asRowSet().rows:
        deletedRows = syn.delete(currentTable.asRowSet())

    # get table schema and set it's release version annotation
    schema = syn.get(tableSynId)
    schema.annotations = {"annotationReleaseVersion": str(releaseVersion)}
    updated_schema_release = syn.store(schema)

    # store the new table on synapse
    table = syn.store(synapseclient.Table(schema, newTable))


def main():
    """
    Given a synapse table id with the schema
        annotation_schema = ["key", "description", "columnType", "maximumSize", "value", "valueDescription",
                         "source", "module"]
    get the most updated release version annotations json files from github Sage-Bionetworks/synapseAnnotations
    normalize the json files per module and create a melted data frame by concatenating all the modules data.
    then upload the melted data frame to the synapse table by completely deleting all rows then replacing content.
    This process also updates the synapse table annotations with the latest release version.


    This code was built under
    Python 2.7.13 :: Anaconda 4.4.0 (x86_64)
    pandas 0.19.2

    Example run:
        python scripts/json2synapse.py --tableId  syn1234 --releaseVersion 'v2.1.1'
    """
    parser = argparse.ArgumentParser('Creates a flattened synapse table from json files located on '
                                     'Sage-Bionetworks/synapseAnnotations/data.')
    parser.add_argument('--tableId', help='A table synapse id containing the annotations',
                        required=False, type=str)
    parser.add_argument('--releaseVersion', help='Sage-Bionetworks/synapseAnnotations release version tag name',
                        required=False, type=str)

    # assign tableSynId from user-input if it exists
    args = parser.parse_args()
    if args.tableId is not None:
        tableSynId = args.tableId
    else:
        tableSynId = "syn10242922"

    if args.releaseVersion is not None:
        releaseVersion = args.releaseVersion
    else:
        releaseVersion = None

    all_modules = []
    key = ["key", "value", "module"]
    annotation_schema = ["key", "description", "columnType", "maximumSize", "value", "valueDescription",
                         "source", "module"]

    names = schema.moduleJsonPath(releaseVersion)

    for module, path in names.iteritems():
        module_df = schema.flattenJson(path, module)
        all_modules.append(module_df)

    # concat the list of all normalized dataframes into one annotation dataframe
    all_modules_df = pandas.concat(all_modules)

    # re-arrange columns/fields and sort data.
    # this step is important for testing (compare_key_values()) where we diff two data frames
    all_modules_df = all_modules_df[annotation_schema]
    all_modules_df.sort_values(key, ascending=[True, True, True], inplace=True)

    # To resolve an error from global searching inside annotationUI R shiny app data tables (https://datatables.net/)
    # I had to convert data encoding here. Currently this issue seems to be resolved, but leaving code in
    # in case of failure. It could also help for testing or creating a table via csv upload
    # to print out the modules data.
    # all_modules_df.to_csv("annot.csv", sep=',', index=False, encoding="utf-8")
    # all_modules_df = pandas.read_csv('annot.csv', delimiter=',', encoding="utf-8")
    # os.remove("annot.csv")

    # Ensure python 2.7 skips or converts special chars such as \u2019 to utf-8.
    # Seems like pandas library does not resolve this issue:
    # UnicodeEncodeError: 'ascii' codec can't encode character u'\u2019' in position x: ordinal not in range(y)
    # Also see: https://docs.python.org/2/howto/unicode.html
    # all_modules_df.valueDescription = all_modules_df.valueDescription.str.encode('ascii', 'ignore')
    all_modules_df.valueDescription = all_modules_df.valueDescription.str.encode('utf-8')

    updateTable(tableSynId=tableSynId, newTable=all_modules_df, releaseVersion=releaseVersion)


if '__main__' == __name__:
    main()
