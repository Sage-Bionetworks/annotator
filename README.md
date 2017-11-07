# annotator &mdash; a Synapse annotations automation tool

 * Streamlines creating file views, adding default annotation values, and linking and merging metadata with data.
 * Keeps changes local until you are ready to push to Synapse. Easily revert back to a previous file view state.
 * Automatic validation methods to help verify that changes propogate correctly and warnings when potential mistakes have been introduced into the annotations.

## Example Usage

We have files in syn1234567 with the following properties:

|name|parentId|...|
|---|---|---|
|MSSM_PFC_1138_Pos_IN_1_TAGCTT_BC90D9ANXX_L002_001.R1.fastq.gz|syn1234567|...|
|PENN_PFC_A10_039_Pos_IN_GGCTAC_AC7JFFANXX_L008_001.R2.fastq.gz|syn1234567|...|
|PITT_PFC_BP_1196_Pos_IN_MERGED.R1.fastq.gz|syn1234567|...|
|randomPythonFile.pyc|syn1234567|...|
|...|...|...|


(Some of) which have metadata in a separate metadata file at syn7654321:

|Individual ID|ChIP_Seq_ID|Histone Mark/Input|NeuN|...|
|---|---|---|---|---|
|CMC_PENN_035|PENN_PFC_A10_039_Pos_IN|Input|Pos|...|
|CMC_PITT_140|PITT_PFC_BP_1196_Pos_IN|H3K4me3|Neg|...|
|CMC_MSSM_038|MSSM_PFC_1138_Pos_IN|Input|Pos|...|
|...|...|....|...|...|

We need to perform the following operations:

 * Add the following annotations to every file: 
     ```
     assay - CHiPSeq
     study - EpiDiff
     consortium - PEC
     PI - Mad Scientist Stu
     host - synapse
     ```
 * Match data columns to metadata columns in the following fashion:
     
     |data|metadata|
     |---|---|
     |specimenID|ChIP_Seq_ID|
     |individualID|Individual ID|
     |assayTarget|Histone Mark/Input|
     |cellType|NeuN|
     
 * cellType should be `NeuN+` or `NeuN-` depending on whether its metadata is `Pos` or `Neg`.
 * Add a file format column, leaving off any compression extensions (like .gz).

Using `annotator`, we can accomplish all of this in a dozen or so lines of code.

```python
In [1]: import synapseclient as sc
In [2]: import annotator
In [3]: syn = sc.login()
Welcome, AnnotatorAdmin!
In [4]: p = annotator.Pipeline(syn, meta="syn7654321")
 [####################]100.00%   1/1   Done...
In [5]: # Copy the default annotations (assay, study, etc.) as they appear above to your clipboard
In [6]: cols = annotator.utils.convertClipboardToDict(" - ")
In [7]: for c in ['specimenID', 'individualID', 'assayTarget', 'cellType']:
            cols[c] = None
In [8]: p.createFileView(name="EpiDiff_CHiPSeq", parent="syn0000000", scope="syn1234567", addCols=cols)
Out[8]: 'syn9999999' # the location of our newly created file view

In [9]: p.addKeyCol()
```
Follow the instructions to link key columns in the data and metadata. The program will output a preview of each column's values and ask for you to input a regular expression to use on the data to match the values in the metadata. A capture group is required.
```
Data 

head
10156185_1    MSSM_PFC_1122_C_CGATGT_AC9FV3ANXX_L006_001.R1....
10156186_1    MSSM_PFC_1122_C_CGATGT_AC9FV3ANXX_L006_001.R2....
10156187_1    MSSM_PFC_1127_C_ACAGTG_BCA8YPANXX_L001_001.R1....
10156189_1    MSSM_PFC_1127_C_ACAGTG_BCA8YPANXX_L001_001.R2....
10156191_1    MSSM_PFC_1129_C_TGACCA_AC9FV1ANXX_L007_001.R1....
Name: name, dtype: object 

tail
10163510_1    PITT_PFC_917_Pos_IN_TAGCTT_ACAADCANXX_L002_00...
10163511_1    PITT_PFC_917_Pos_IN_TAGCTT_ACAADCANXX_L001_001...
10163512_1    PITT_PFC_917_Pos_IN_TAGCTT_ACAADCANXX_L001_001...
10163513_1           PITT_PFC_1196_Pos_IN_MERGED.R1.fastq.gz
10163514_1           PITT_PFC_1196_Pos_IN_MERGED.R2.fastq.gz
Name: name, dtype: object 


Metadata 

0         PITT_PFC_516_A
1         PITT_PFC_516_B
2         PITT_PFC_516_C
3         PITT_PFC_516_D
4    PITT_PFC_516_Pos_IN
Name: ChIP_Seq_ID, dtype: object 

regex: ^([A-Z]+_.+_\d+_(?:(?:Pos_IN)|[A-Z]))_

The following values were not found in the metadata:
PENN_A10_040_C <- PENN_A10_040_C_CAGATC_BC7TY5ANXX_L008_001.R1.fastq.gz
None <- randomPythonFile.pyc

Proceed anyways? (y) or (n): y
```
In some cases, files are missing metadata or whoever uploaded the data included files that you don't want to annotate. The program will print out the value that was parsed via the regular expression and its source to the right. If we decide that it was our regular expression that erred, we can input `n` and the program will have us try a new regular expression. The previously entered regular expression will already be written to the input line, so you won't have to type the whole thing from scratch. Arrow keys can be used to go further back in input history.

If we accept (`y`), then a new column with the same name as the key column in the metadata (`ChIP_Seq_ID`) will be added to our data view, containing the results of mapping the regular expression to the key column in the data (`name`). If our regular expression was able to find every value it parsed in the metadata, then there would be no output from the program -- we would simply jump right back into the console with our newly created column.

Now that we have a column in both the data and the metadata to align upon, we can link each column requiring annotations in the data with its respective column in the metadata. Input a newline once you are finished.

```python
In [10]: p.addLinks() # Same process as p.addKeyCol, but no regular expressions this time :-)
In [11]: p.transferLinks()
original: (323, 10) # we already have a ChIP_Seq_ID column here
merged: (323, 13)
```
Transfering the metadata to the data outputs the shape of our view before and after the table merge. This is useful for verifying that no files were duplicated in the data view because of redundant keys in the metadata, and that all the columns in the metadata we want to copy over to the data were included in the merge.

After copying the values from the metadata to their respective columns in the data, the program will automatically drop both the key column and the metadata columns from the data view.

Remember, we still have to add a file format column and transform the values in `cellType`.

```python
In [12]: p.addFileFormatCol()
In [13]: p.substituteColumnValues('cellType', {'Pos':'NeuN+', 'Neg':'NeuN-'})
```
The `addFileFormatCol` method defaults to a preprogrammed regex and uses column 'name' in the data view. But we can use a more flexible approach by using `annotator.utils.makeColFromRegex` if necessary.

If everything looks good, we can go ahead and push our changes back up to Synapse.

```python
In [14]: p.publish()

specimenID has null values.
individualID has null values.
assayTarget has null values.
cellType has null values.

Proceed anyways? (y) or (n): y
```

Because we explicitly added the above columns when we created the file view, the program assumes we meant to fill them completely with values. If any rows are missing values for these `activeColumns`, then the program will warn you before trying to push to Synapse (and reindex your view to keep the indices consistent between your local machine and Synapse). We already know that some values will be missing because some files are missing metadata, so we proceed with the push anyways.

And we're done.
