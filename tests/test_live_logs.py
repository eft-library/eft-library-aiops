import json
import os

import pytest
from fastapi.testclient import TestClient

from aiops.api import create_app
from aiops.config import Settings
from aiops.live_logs import LogViewer


@pytest.fixture
def logs(tmp_path):
    source=tmp_path/'app.log';source.write_text('INFO first\nERROR second\n')
    config=tmp_path/'sources.yaml';config.write_text('sources:\n  fastapi:\n    type: file\n    path: app.log\nsystem:\n  enabled: false\n')
    viewer=LogViewer(Settings(data_dir=tmp_path/'data'),config,'private-api-key')
    return viewer,source,config


def test_viewer_masks_and_reads_appends_without_checkpoints(logs,monkeypatch):
    viewer,source,_=logs
    monkeypatch.setenv('WEB_PASSWORD','very-private-secret')
    # Rebuild cleaner after adding secrets, as at server startup.
    viewer=LogViewer(viewer.settings,viewer.config_path,'private-api-key')
    source.write_text('INFO password=very-private-secret\nINFO partial token=split-secret')
    first=viewer.read('fastapi')
    assert 'very-private-secret' not in json.dumps(first)
    assert len(first['entries'])==1
    source.write_text(source.read_text()+'-finished\n')
    second=viewer.read('fastapi',cursor=first['cursor'])
    assert len(second['entries'])==1 and 'split-secret' not in json.dumps(second)
    assert viewer.read('fastapi',cursor=second['cursor'])['entries']==[]
    assert not viewer.settings.data_dir.exists()


def test_rotation_rewrite_invalid_cursor_and_unknown_target(logs):
    viewer,source,_=logs
    first=viewer.read('fastapi');source.rename(source.with_suffix('.old'));source.write_text('WARNING new log\n')
    next=viewer.read('fastapi',cursor=first['cursor'])
    assert next['entries'][0]['text']=='WARNING new log'
    assert 'file_rotated_or_truncated' in next['notices']
    with pytest.raises(ValueError):viewer.read('fastapi',cursor=first['cursor']+'bad')
    with pytest.raises(ValueError):viewer.read('../../etc/passwd')
    source.unlink();source.symlink_to(source.with_suffix('.old'))
    with pytest.raises(FileNotFoundError):viewer.read('fastapi')


def test_directory_selects_bounded_files_and_follows_new_active_file(tmp_path):
    logs=tmp_path/'logs';logs.mkdir();old=logs/'old.log';old.write_text('INFO old\n');os.utime(old,(1,1))
    config=tmp_path/'sources.yaml';config.write_text('sources:\n  airflow:\n    type: directory\n    path: logs\n    recursive: true\nsystem:\n  enabled: false\n')
    viewer=LogViewer(Settings(data_dir=tmp_path/'data'),config,'secret')
    first=viewer.read('airflow')
    new=logs/'new.log';new.write_text('INFO current\n')
    after=viewer.read('airflow',cursor=first['cursor'])
    assert 'active_file_changed' in after['notices']
    assert after['entries'][0]['text']=='INFO current'
    listing=viewer.listing('airflow')
    assert all(str(tmp_path) not in item['label'] for item in listing['items'])
    assert viewer.read('airflow',listing['items'][1]['file_id'])['entries'][0]['text']=='INFO old'


def test_log_api_requires_key_and_rejects_path_selection(logs):
    viewer,_,config=logs
    client=TestClient(create_app(Settings(database_url='dummy'),token='private-api-key',sources_config=config))
    assert client.get('/v1/logs/services').status_code==401
    headers={'X-AIOps-Key':'private-api-key'}
    assert client.get('/v1/logs/services',headers=headers).json()['items']==[{'service':'fastapi'}]
    assert client.get('/v1/logs/tail',params={'service':'fastapi','file_id':'/etc/passwd'},headers=headers).status_code==422
    assert client.get('/v1/logs/tail',params={'service':'fastapi'},headers=headers).json()['entries']


def test_oversized_partial_line_is_not_exposed(logs):
    viewer,source,_=logs
    source.write_text('INFO '+('s'*100000))
    first=viewer.read('fastapi')
    assert first['entries']==[]
    with source.open('a') as f:f.write('continued\nINFO safe\n')
    after=viewer.read('fastapi',cursor=first['cursor'])
    assert [row['text'] for row in after['entries']]==['INFO safe']
