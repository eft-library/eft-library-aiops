import argparse
import json
import sqlite3
import sys
from pathlib import Path

from dotenv import load_dotenv

from aiops.config import Settings
from aiops.providers.base import AIProvider, AnalysisRequest, ErrorKind, ProviderError
from aiops.providers.codex import CodexProvider, doctor

EXIT_CODES = {
    ErrorKind.CONFIG: 2,
    ErrorKind.AUTH: 3,
    ErrorKind.NETWORK: 4,
    ErrorKind.RATE_LIMIT: 5,
    ErrorKind.TIMEOUT: 6,
    ErrorKind.BUSY: 7,
    ErrorKind.OUTPUT: 8,
    ErrorKind.EXECUTION: 9,
}


def main(argv=None, *, provider: AIProvider | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only EFT Library log analysis")
    parser.add_argument(
        "--env-file", type=Path, help="Explicit dotenv file (existing environment wins)"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("doctor", help="Check local Python, Codex and login status")
    analyze = commands.add_parser(
        "analyze", help="Analyze a sanitized sample (real Codex call)"
    )
    analyze.add_argument("--input", type=Path, required=True)
    collect = commands.add_parser(
        "collect", help="Collect logs and metrics without Codex"
    )
    selection = collect.add_mutually_exclusive_group(required=True)
    selection.add_argument(
        "--all", action="store_true", help="All configured log sources"
    )
    selection.add_argument(
        "--service", action="append", help="Source name; repeat for multiple sources"
    )
    collect.add_argument(
        "--config",
        type=Path,
        default=Path("config/log_sources.fixture.yaml"),
        help="YAML profile; defaults to local fixtures",
    )
    collect.add_argument(
        "--since", help="Filter newly read timestamps, e.g. 24h (not a historical seek)"
    )
    collect.add_argument(
        "--backfill",
        action="store_true",
        help="Start unseen files at byte zero (still bounded)",
    )
    args = parser.parse_args(argv)
    try:
        if args.env_file:
            if not args.env_file.is_file():
                raise ProviderError(ErrorKind.CONFIG)
            load_dotenv(args.env_file, override=False)
        settings = Settings.from_env()
        if args.command == "doctor":
            output = doctor(settings)
        elif args.command == "collect":
            from aiops.collectors.config import load_config
            from aiops.collectors.service import CollectionService

            result = CollectionService(
                load_config(args.config), settings.data_dir
            ).collect(args.service, since=args.since, backfill=args.backfill)
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 10 if result["status"] == "partial" else 0
        else:
            # Bound the read even for unexpectedly large files; never expose paths/content in errors.
            with args.input.open("rb") as stream:
                raw = stream.read(settings.max_input_bytes + 1)
            if not raw or len(raw) > settings.max_input_bytes:
                raise ProviderError(ErrorKind.CONFIG)
            snapshot = raw.decode("utf-8")
            json.loads(snapshot)  # Phase 1 accepts sanitized JSON snapshots only.
            output = (
                (provider or CodexProvider(settings))
                .analyze(AnalysisRequest(snapshot))
                .model_dump()
            )
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0
    except ProviderError as exc:
        print(json.dumps({"error": exc.kind.value}), file=sys.stderr)
        return EXIT_CODES[exc.kind]
    except (OSError, ValueError, sqlite3.Error):
        print(json.dumps({"error": ErrorKind.CONFIG.value}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
