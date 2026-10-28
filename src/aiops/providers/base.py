from dataclasses import dataclass
from enum import StrEnum
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict


class ErrorKind(StrEnum):
    DATABASE = 'database'
    CONFIG = 'configuration'
    AUTH = 'authentication'
    NETWORK = 'network'
    RATE_LIMIT = 'rate_limit'
    TIMEOUT = 'timeout'
    BUSY = 'busy'
    OUTPUT = 'invalid_output'
    EXECUTION = 'execution'


class ProviderError(Exception):
    def __init__(self, kind: ErrorKind):
        self.kind = kind
        super().__init__(kind.value)


class Finding(BaseModel):
    model_config = ConfigDict(extra='forbid')
    issue: str
    evidence_ids: list[str]
    hypothesis: str
    recommended_checks: list[str]


class AnalysisSection(BaseModel):
    model_config = ConfigDict(extra='forbid')
    status: Literal['ISSUES_OBSERVED', 'NO_ISSUES_OBSERVED', 'INSUFFICIENT_DATA']
    summary: str
    findings: list[Finding]
    limitations: list[str]


class Report(BaseModel):
    model_config = ConfigDict(extra='forbid')
    summary: str
    severity: Literal['OK', 'INFO', 'WARNING', 'CRITICAL']
    findings: list[Finding]
    limitations: list[str]
    automatic_changes_performed: Literal[False]
    # Older stored reports did not have these sections.
    incident_analysis: AnalysisSection | None = None
    performance_analysis: AnalysisSection | None = None


@dataclass(frozen=True)
class AnalysisRequest:
    snapshot: str


class AnalysisResult(BaseModel):
    job_id: str
    provider: str
    model: str | None
    thread_id: str | None
    report: Report


class AIProvider(Protocol):
    def analyze(self, request: AnalysisRequest) -> AnalysisResult: ...
