"""Environment configuration; .env loading is an explicit CLI choice."""
import math
import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    codex_bin: str = 'codex'
    data_dir: Path = Path('aiops_data')
    timeout: float = 600
    max_input_bytes: int = 100_000
    model: str | None = None

    @classmethod
    def from_env(cls) -> 'Settings':
        settings = cls(
            codex_bin=os.getenv('AIOPS_CODEX_BIN', 'codex'),
            data_dir=Path(os.getenv('AIOPS_DATA_DIR', 'aiops_data')).expanduser().resolve(),
            timeout=float(os.getenv('AIOPS_TIMEOUT_SECONDS', '600')),
            max_input_bytes=int(os.getenv('AIOPS_MAX_INPUT_BYTES', '100000')),
            model=os.getenv('AIOPS_MODEL') or None,
        )
        if not math.isfinite(settings.timeout) or settings.timeout <= 0:
            raise ValueError('AIOPS_TIMEOUT_SECONDS must be finite and positive')
        if settings.max_input_bytes <= 0:
            raise ValueError('AIOPS_MAX_INPUT_BYTES must be positive')
        return settings
