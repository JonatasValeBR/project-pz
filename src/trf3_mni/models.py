from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


@dataclass(frozen=True)
class ReportWindow:
    start: date
    end: date

    def __post_init__(self) -> None:
        if self.end < self.start:
            raise ValueError('A data final não pode ser anterior à data inicial')


@dataclass(frozen=True)
class DocumentReference:
    document_id: str
    document_type: str


@dataclass(frozen=True)
class ProcessMetadata:
    process_number: str
    filing_date: str
    class_code: str
    municipality_code: str
    documents: List[DocumentReference]


@dataclass(frozen=True)
class DownloadedDocument:
    reference: DocumentReference
    content: bytes


@dataclass(frozen=True)
class StoredArtifact:
    path: Union[Path, str]
    sha256: str
    size_bytes: int
    reused: bool

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result['path'] = str(self.path)
        return result


@dataclass
class ProcessResult:
    process_number: str
    status: str
    artifacts: List[StoredArtifact] = field(default_factory=list)
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'process_number': self.process_number,
            'status': self.status,
            'artifacts': [artifact.to_dict() for artifact in self.artifacts],
            'error_type': self.error_type,
            'error_message': self.error_message,
            'started_at': (
                self.started_at.astimezone(timezone.utc).isoformat()
                if self.started_at else None
            ),
            'finished_at': (
                self.finished_at.astimezone(timezone.utc).isoformat()
                if self.finished_at else None
            ),
            'duration_seconds': self.duration_seconds,
        }


@dataclass
class BatchManifest:
    window: ReportWindow
    started_at: datetime
    finished_at: datetime
    results: List[ProcessResult]
    report_artifacts: List[StoredArtifact] = field(default_factory=list)
    run_id: Optional[str] = None
    source_manifest: Optional[str] = None

    @property
    def succeeded(self) -> int:
        return sum(result.status == 'success' for result in self.results)

    @property
    def failed(self) -> int:
        return sum(result.status == 'failed' for result in self.results)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'window': {
                'start': self.window.start.isoformat(),
                'end': self.window.end.isoformat(),
            },
            'run_id': self.run_id,
            'source_manifest': self.source_manifest,
            'started_at': self.started_at.astimezone(timezone.utc).isoformat(),
            'finished_at': self.finished_at.astimezone(timezone.utc).isoformat(),
            'summary': {
                'total': len(self.results),
                'succeeded': self.succeeded,
                'failed': self.failed,
            },
            'report_artifacts': [
                artifact.to_dict() for artifact in self.report_artifacts
            ],
            'results': [result.to_dict() for result in self.results],
        }
