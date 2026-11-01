"""Bounded log viewer. Never touches Collector checkpoints or accepts filesystem paths."""
import fnmatch
import base64
import hashlib
import hmac
import heapq
import json
import os
import re
import time
from pathlib import Path

from aiops.collectors.config import load_config
from aiops.collectors.parse import LEVEL
from aiops.investigation.probes import open_regular
from aiops.providers.progress import PublicProgress


class LogViewer:
    def __init__(self,settings,config_path,secret):
        self.settings=settings
        self.config_path=config_path
        self.secret=secret.encode()
        self.clean=PublicProgress(settings,lambda event:None,extra_secrets=(secret,)).clean

    def identifier(self,service,path):
        return hmac.new(self.secret,(service+'\0'+str(path)).encode(),hashlib.sha256).hexdigest()[:32]

    def files(self,service):
        config=load_config(self.config_path)
        source=config.sources.get(service)
        if source is None or not source.enabled:raise ValueError('unknown service')
        paths=[];scanned=0;limited=False;deadline=time.monotonic()+.5
        pending=[(-source.path.stat().st_mtime, str(source.path))]
        while pending:
            if scanned>=1024 or time.monotonic()>=deadline:limited=True;break
            _, value=heapq.heappop(pending);path=Path(value);scanned+=1
            if path.is_symlink() or any(parent.is_symlink() for parent in path.parents):continue
            if path.is_dir():
                if source.type=='file':continue
                with os.scandir(path) as entries:
                    for entry in entries:
                        scanned+=1
                        if scanned>=1024 or time.monotonic()>=deadline:limited=True;break
                        if entry.is_symlink():continue
                        if entry.is_dir(follow_symlinks=False):
                            if source.recursive:heapq.heappush(pending,(-entry.stat(follow_symlinks=False).st_mtime,entry.path))
                        elif entry.is_file(follow_symlinks=False) and fnmatch.fnmatch(entry.name,source.pattern) and not entry.name.endswith(('.gz','.zip','.bz2','.xz')):
                            info=entry.stat(follow_symlinks=False);paths.append((Path(entry.path),info.st_mtime))
            elif path.is_file():paths.append((path,path.stat().st_mtime))
        paths.sort(key=lambda item:(-item[1],str(item[0])))
        return source,paths[:50],limited or len(paths)>50

    def services(self):
        config=load_config(self.config_path)
        return {'items':[{'service':name} for name,source in config.sources.items() if source.enabled]}

    def listing(self,service):
        source,paths,limited=self.files(service)
        return {'items':[{'file_id':self.identifier(service,path),
                          'label':self.clean(str(path.relative_to(source.path) if source.type=='directory' else Path(path.name))),
                          'modified_at':stamp} for path,stamp in paths],
                'limited':limited,'scope':'Bounded recent-file listing; not all files.'}

    def sign(self,data):
        payload=base64.urlsafe_b64encode(json.dumps(data,separators=(',',':')).encode()).decode()
        return payload+'.'+hmac.new(self.secret,payload.encode(),hashlib.sha256).hexdigest()

    def decode(self,cursor,file_id):
        if not cursor:return None
        if len(cursor)>1024 or not re.fullmatch(r'[A-Za-z0-9_=-]+\.[a-f0-9]{64}',cursor):raise ValueError('invalid cursor')
        payload,sep,signature=cursor.partition('.')
        if not sep or not hmac.compare_digest(signature,hmac.new(self.secret,payload.encode(),hashlib.sha256).hexdigest()):raise ValueError('invalid cursor')
        try:
            data=json.loads(base64.urlsafe_b64decode(payload))
            if (file_id is not None and data['file']!=file_id) or type(data['offset']) is not int or data['offset']<0:raise ValueError('invalid cursor')
            return data
        except (ValueError,KeyError,TypeError):raise ValueError('invalid cursor') from None

    def read(self,service,file_id=None,cursor=None):
        source,paths,limited=self.files(service)
        if not paths:raise FileNotFoundError('no readable file')
        selected=next((path for path,_ in paths if self.identifier(service,path)==file_id),None) if file_id else paths[0][0]
        if selected is None:raise FileNotFoundError('file outside bounded listing')
        actual_id=self.identifier(service,selected)
        prior=self.decode(cursor,actual_id if file_id else None)
        switched=bool(prior and prior['file']!=actual_id)
        if switched:prior=None
        fd=open_regular(selected)
        notices=['active_file_changed'] if switched else []
        try:
            info=os.fstat(fd);offset=max(0,info.st_size-65536);skip=False;initial=True
            if prior and (prior['dev'],prior['ino'])==(info.st_dev,info.st_ino) and prior['offset']<=info.st_size:
                offset=prior['offset'];os.lseek(fd,max(0,offset-32),os.SEEK_SET)
                boundary=hashlib.sha256(os.read(fd,min(32,offset))).hexdigest()
                if boundary==prior['boundary']:
                    skip=prior.get('skip',False);initial=False
                else:offset=max(0,info.st_size-65536);notices.append('file_rewritten')
            elif prior:notices.append('file_rotated_or_truncated')
            os.lseek(fd,offset,os.SEEK_SET);raw=os.read(fd,65536)
            if initial and offset:
                split=raw.find(b'\n')
                if split>=0:offset+=split+1;raw=raw[split+1:]
                else:skip=True
            last=raw.rfind(b'\n')
            if last<0:
                if len(raw)>=65536:offset+=len(raw);skip=True;notices.append('oversized_line_skipped')
                complete=b''
            else:
                complete=raw[:last+1];offset+=last+1
                if skip:complete=complete.partition(b'\n')[2];skip=False;notices.append('oversized_line_skipped')
            lines=complete.decode('utf-8',errors='replace').splitlines()
            if len(lines)>300:notices.append('display_limit_older_lines_omitted');lines=lines[-300:]
            entries=[]
            for line in lines:
                # Mask full complete lines before limiting output; never expose partial tokens.
                text=self.clean(line)
                level_match=LEVEL.search(line)
                level=level_match.group(1).upper() if level_match else 'UNKNOWN'
                if level=='WARN':level='WARNING'
                entries.append({'level':level,'text':text})
            os.lseek(fd,max(0,offset-32),os.SEEK_SET)
            boundary=hashlib.sha256(os.read(fd,min(32,offset))).hexdigest()
            token=self.sign({'file':actual_id,'dev':info.st_dev,'ino':info.st_ino,'offset':offset,'boundary':boundary,'skip':skip})
            return {'service':service,'file_id':actual_id,'entries':entries,'cursor':token,
                    'notices':notices,'listing_limited':limited,'initial':initial}
        finally:os.close(fd)
