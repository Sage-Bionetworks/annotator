import pytest
import sys
import logging
import pandas
import synapseclient
import uuid


SAMPLE_FILE = "../sampleFile.csv"
SAMPLE_META = "../sampleMeta.csv"


@pytest.fixture(scope='session')
def syn():
    syn = synapseclient.login()
    return syn


@pytest.fixture(scope='session')
def sampleFile():
    data = pandas.read_csv(SAMPLE_FILE, header=0, index_col=None)
    return data


@pytest.fixture(scope='session')
def sampleMetadata():
    meta = pandas.read_csv(SAMPLE_META, header=0, index_col=None)
    return meta


@pytest.fixture(scope='session')
def project(syn):
    project = synapseclient.Project(str(uuid.uuid4()))
    project = syn.store(project)
    yield project
    syn.delete(project)


def file_(syn, parent, annotations=None, **kwargs):
    if 'name' not in kwargs:
        name = str(uuid.uuid4())
    else:
        name = kwargs.pop('name')
    file_ = synapseclient.File(path=SAMPLE_FILE, name=name,
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


@pytest.fixture(scope='session')
def entities(syn, sampleFile, project):
    # store a folder for our entities
    sample_folder = folder(syn, project)
    # store sample files
    _file = file_(syn, sample_folder, name="file1.csv")
    _file2 = file_(syn, sample_folder, name="file2.csv")
    _file3 = file_(syn, sample_folder, name="file3.csv")
    # store a sample metadata file
    meta = synapseclient.File(path=SAMPLE_META, name='meta',
                              parent=sample_folder)
    meta = syn.store(meta)
    # store a sample table (same values as sample file)
    df = sampleFile
    cols = synapseclient.as_table_columns(df)
    schema = synapseclient.Schema(name=str(uuid.uuid4()), columns=cols,
                                  parent=project)
    schema = syn.store(schema)
    table = syn.store(synapseclient.Table(schema, df))
    # store a sample file view
    entity_view_ = entity_view(syn, project, scopes=sample_folder)
    ents = {'files': [_file, _file2, _file3],
            'folder': sample_folder,
            'meta': meta,
            'table_schema': schema,
            'entity_view': entity_view_}
    return ents
