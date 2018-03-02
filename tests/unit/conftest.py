# -*- coding: utf-8 -*-

import pytest
import sys
import logging
import tempfile
import pandas
import synapseclient
import uuid
import StringIO

SAMPLE_FILE = "https://raw.githubusercontent.com/Sage-Bionetworks/annotator/master/tests/sampleFile.csv"
SAMPLE_META = "https://raw.githubusercontent.com/Sage-Bionetworks/annotator/master/tests/sampleMeta.csv"
SAMPLE_FILE_LOCAL_PATH = None
SAMPLE_META_LOCAL_PATH = None


@pytest.fixture(scope='session')
def syn():
    syn = synapseclient.login()
    return syn


def read(obj):
    if isinstance(obj, str):
        return pandas.read_csv(obj, header=0, index_col=None)
    else:
        return obj


@pytest.fixture(scope='session')
def sampleFile():
    sampleFileText = "favoriteColor,name\nblue,phil\ngreen,tom"
    f = StringIO.StringIO(sampleFileText)
    return pandas.read_csv(f, header=0, index_col=None)


@pytest.fixture(scope='session')
def sampleMetadata():

    sampleMetadataText = "id,mexico,serbia,favoriteMeat,favoriteFruit,team\n1,quien,ко,bacon,orange,blue\n2,que,Шта,bacon,banana,blue\n3,donde,где,,,blue"
    f = StringIO.StringIO(sampleFileText)
    return pandas.read_csv(f, header=0, index_col=None)


@pytest.fixture(scope='session')
def project(syn):
    project = synapseclient.Project(str(uuid.uuid4()))
    project = syn.store(project)
    yield project
    syn.delete(project)


def file_(syn, parent, path=None, annotations=None, **kwargs):
    global SAMPLE_FILE_LOCAL_PATH
    if path is None:
        if SAMPLE_FILE_LOCAL_PATH is not None:
            path = SAMPLE_FILE_LOCAL_PATH
        else:
            path = tempfile.mkstemp()[1]
            df = sampleFile()
            df.to_csv(path, index=False, header=True)
            SAMPLE_FILE_LOCAL_PATH = path
    name = str(uuid.uuid4()) if 'name' not in kwargs else kwargs.pop('name')
    file_ = synapseclient.File(path=path, name=name,
                               parent=parent, **kwargs)
    if annotations:
        for a in annotations.items():
            key, value = a
            file_[key] = value
    file_ = syn.store(file_)
    return file_


@pytest.fixture
def genericFile(syn, project):
    return _file(syn, project)


def folder(syn, parent):
    folder = synapseclient.Folder(str(uuid.uuid4()), parent=parent)
    folder = syn.store(folder)
    return folder


@pytest.fixture
def genericFolder(syn, project):
    return _folder(syn, project)


def entity_view(syn, project, scopes=None):
    scopes = [project] if scopes is None else scopes
    entity_view = synapseclient.EntityViewSchema(
            name=str(uuid.uuid4()),
            parent=project,
            scopes=scopes)
    entity_view = syn.store(entity_view)
    return entity_view


@pytest.fixture
def genericEntityView(syn, project):
    return entity_view(syn, project)


def table(syn, parent, obj):
    df = read(obj)
    cols = synapseclient.as_table_columns(df)
    schema = synapseclient.Schema(name=str(uuid.uuid4()), columns=cols,
                                  parent=parent)
    schema = syn.store(schema)
    table = syn.store(synapseclient.Table(schema, df))
    return schema


@pytest.fixture
def genericTable(syn, project, sampleFile):
    return table(syn, project, sampleFile)


@pytest.fixture(scope='session')
def entities(syn, sampleFile, project):
    # store a folder for our entities
    sample_folder = folder(syn, project)
    # store sample files
    _file = file_(syn, sample_folder, name="file1.csv")
    _file2 = file_(syn, sample_folder, name="file2.csv")
    _file3 = file_(syn, sample_folder, name="file3.csv")
    # store a sample metadata file
    meta = file_(syn, sample_folder, name='meta.csv')
    # store a sample table (same values as sample file)
    schema = table(syn, project, sampleFile)
    # store a sample file view
    entity_view_ = entity_view(syn, project, scopes=sample_folder)
    ents = {'files': [_file, _file2, _file3],
            'folder': sample_folder,
            'meta': meta,
            'table_schema': schema,
            'entity_view': entity_view_}
    return ents
