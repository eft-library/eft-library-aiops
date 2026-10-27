import argparse
import json
import sys
from pathlib import Path

from dotenv import load_dotenv

from aiops.config import Settings
from aiops.providers.base import AIProvider, AnalysisRequest, ErrorKind, ProviderError
from aiops.providers.codex import CodexProvider, doctor

EXIT_CODES = {ErrorKind.CONFIG: 2, ErrorKind.AUTH: 3, ErrorKind.NETWORK: 4,
              ErrorKind.RATE_LIMIT: 5, ErrorKind.TIMEOUT: 6, ErrorKind.BUSY: 7,
              ErrorKind.OUTPUT: 8, ErrorKind.EXECUTION: 9}


def main(argv=None, *, provider: AIProvider | None = None) -> int:
    parser = argparse.ArgumentParser(description='Read-only EFT Library log analysis')
    parser.add_argument('--env-file', type=Path, help='Explicit dotenv file (existing environment wins)')
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('doctor', help='Check local Python, Codex and login status')
    analyze = commands.add_parser('analyze', help='Analyze a sanitized sample (real Codex call)')
    analyze.add_argument('--input', type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.env_file:
            if not args.env_file.is_file():
                raise ProviderError(ErrorKind.CONFIG)
            load_dotenv(args.env_file, override=False)
        settings = Settings.from_env()
        if args.command == 'doctor':
            output = doctor(settings)
        else:
            # Bound the read even for unexpectedly large files; never expose paths/content in errors.
            with args.input.open('rb') as stream:
                raw = stream.read(settings.max_input_bytes + 1)
            if not raw or len(raw) > settings.max_input_bytes:
                raise ProviderError(ErrorKind.CONFIG)
            snapshot = raw.decode('utf-8')
            json.loads(snapshot)  # Phase 1 accepts sanitized JSON snapshots only.
            output = (provider or CodexProvider(settings)).analyze(AnalysisRequest(snapshot)).model_dump()
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0
    except ProviderError as exc:
        print(json.dumps({'error': exc.kind.value}), file=sys.stderr)
        return EXIT_CODES[exc.kind]
    except (OSError, ValueError):
        print(json.dumps({'error': ErrorKind.CONFIG.value}), file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
