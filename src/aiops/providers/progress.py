"""Allowlisted public CLI events, never reasoning or raw stderr/tool output."""

import json
import os
import re
from urllib.parse import quote

from psycopg.conninfo import conninfo_to_dict

from aiops.collectors.redact import REDACTED, mask
from aiops.providers.instructions import classify


class PublicProgress:
    def __init__(self, settings, emit, *, extra_secrets=()):
        self.emit = emit
        self.secrets = {value for key, value in os.environ.items()
                        if value and re.search(r'TOKEN|KEY|PASSWORD|SECRET|DATABASE_URL|DSN', key, re.I)}
        self.secrets.update(value for value in extra_secrets if value)
        if settings.database_url:
            self.secrets.add(settings.database_url)
            try:
                password = conninfo_to_dict(settings.database_url).get('password')
                if password:
                    self.secrets.update((password, quote(password, safe='')))
            except Exception:
                pass
        self.seen_messages = set()

    def clean(self, text):
        text = re.sub(r'\x1b\[[0-?]*[ -/]*[@-~]', '', text)
        text = re.sub(r'[\x00-\x08\x0b-\x1f\x7f]', '', text)
        for secret in sorted(self.secrets, key=len, reverse=True):
            text = text.replace(secret, REDACTED)
        # Mask before truncation so split credentials cannot escape the masker.
        text = mask(text)
        return text[:4000] + (' [truncated]' if len(text) > 4000 else '')

    def line(self, line):
        try:
            event = json.loads(line)
        except (ValueError, TypeError):
            return  # The provider still rejects malformed final output normally.
        if not isinstance(event, dict):
            return
        kind = event.get('type')
        if kind == 'thread.started':
            thread = event.get('thread_id')
            self.emit({'event': 'codex.thread.started',
                       'thread_id': self.clean(thread) if isinstance(thread, str) else None})
        elif kind == 'turn.started':
            self.emit({'event': 'codex.turn.started'})
        elif kind == 'turn.completed':
            usage = event.get('usage') or {}
            allowed = {key: usage[key] for key in (
                'input_tokens', 'cached_input_tokens', 'output_tokens', 'reasoning_output_tokens'
            ) if isinstance(usage, dict) and type(usage.get(key)) is int and usage[key] >= 0}
            self.emit({'event': 'codex.turn.completed', 'usage': allowed})
        elif kind in {'error', 'turn.failed'}:
            self.emit({'event': 'codex.error', 'error': classify(json.dumps(event)).value})
        elif kind in {'item.started', 'item.updated', 'item.completed'}:
            item = event.get('item')
            if not isinstance(item, dict):
                return
            item_type = item.get('type')
            # In particular: never expose reasoning/analysis item text.
            if item_type == 'agent_message' and kind == 'item.completed':
                text = item.get('text')
                if not isinstance(text, str) or not text.strip():
                    return
                try:
                    candidate = json.loads(text)
                except ValueError:
                    candidate = None
                if isinstance(candidate, dict) and {'summary', 'findings'} <= candidate.keys():
                    self.emit({'event': 'codex.report_candidate',
                               'message': '구조화된 보고서 응답을 수신했습니다. 검증과 저장은 아직 완료되지 않았습니다.'})
                else:
                    # Only complete public messages, not partial deltas containing split secrets.
                    identity = (item.get('id') if isinstance(item.get('id'), str) else None, text)
                    if identity not in self.seen_messages and len(self.seen_messages) < 1000:
                        self.seen_messages.add(identity)
                        self.emit({'event': 'codex.message', 'message': self.clean(text)})
            elif item_type in {'command_execution', 'file_change', 'mcp_tool_call', 'web_search', 'todo_list'}:
                self.emit({'event': 'codex.activity', 'item_type': item_type,
                           'phase': kind.split('.')[-1]})
