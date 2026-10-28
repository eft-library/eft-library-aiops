import fnmatch
import hashlib
import os
import re
import stat
import time
from dataclasses import dataclass, field
from pathlib import Path

from aiops.collectors.config import CollectionConfig, Source

COMPRESSED = (".gz", ".bz2", ".xz", ".zip")
ROTATED = re.compile(r"\.(?:\d+|bak|old|\d{4}-\d{2}-\d{2})$")


class ScanError(Exception):
    def __init__(self, reason):
        self.reason = reason
        super().__init__(reason)


@dataclass
class ReadResult:
    lines: list = field(default_factory=list)
    checkpoint: dict | None = None
    bytes_read: int = 0
    warnings: list[str] = field(default_factory=list)


def fingerprint(stream, start, length):
    stream.seek(start)
    return hashlib.sha256(stream.read(length)).hexdigest()


class FileCollector:
    def __init__(self, source: Source, config: CollectionConfig):
        self.source, self.config = source, config
        self.warnings = []

    def candidates(self, known, deadline):
        path = self.source.path
        # Only exact active file + already tracked renamed inode siblings by default.
        if not stat.S_ISDIR(path.parent.stat().st_mode):
            raise ScanError("not_directory")
        candidates = []
        seen = 0
        with os.scandir(path.parent) as entries:
            for entry in entries:
                seen += 1
                if seen > self.config.max_scan_entries or time.monotonic() >= deadline:
                    self.warnings.append("scan_limit")
                    break
                if entry.name != path.name and not entry.name.startswith(
                    path.name + "."
                ):
                    continue
                if entry.is_symlink():
                    if entry.name == path.name:
                        self.warnings.append("symlink_excluded")
                    continue
                info = entry.stat(follow_symlinks=False)
                if not stat.S_ISREG(info.st_mode) or entry.name.lower().endswith(
                    COMPRESSED
                ):
                    continue
                tracked = (info.st_dev, info.st_ino) in known
                if (
                    entry.name == path.name
                    or tracked
                    or (self.source.include_rotated and ROTATED.search(entry.name))
                ):
                    candidates.append((Path(entry.path), info))
        if not any(candidate == path for candidate, _ in candidates):
            self.warnings.append("active_file_missing")
        return self.ordered(candidates, known)

    @staticmethod
    def ordered(candidates, known):
        def priority(pair):
            path, info = pair
            previous = known.get((info.st_dev, info.st_ino))
            if previous is None:
                return (1, "", info.st_mtime_ns, str(path))
            changed = (
                previous["offset"] < info.st_size
                or previous["mtime_ns"] != info.st_mtime_ns
            )
            return (
                0 if changed else 2,
                previous["last_success"],
                info.st_mtime_ns,
                str(path),
            )

        # Drain known backlog, discover new files, then revisit unchanged files.
        return sorted(candidates, key=priority)

    def read(
        self,
        path,
        expected,
        previous,
        budget,
        deadline,
        backfill=False,
        max_lines=20000,
    ):
        result = ReadResult()
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | os.O_NONBLOCK
        fd = os.open(path, flags)
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode):
                raise ScanError("not_regular_file")
            if (info.st_dev, info.st_ino) != (expected.st_dev, expected.st_ino):
                raise ScanError("file_replaced_during_open")
            initial_prefix_length = min(info.st_size, 256)
            initial_prefix = fingerprint(stream, 0, initial_prefix_length)
            offset, dropping = 0, False
            if previous and previous.get("path") != str(path):
                result.warnings.append("renamed_file_followed")
            if previous:
                offset = previous["offset"]
                unchanged = (
                    info.st_size >= offset
                    and info.st_mtime_ns >= previous["mtime_ns"]
                    and not (
                        info.st_size == previous["size"]
                        and info.st_mtime_ns != previous["mtime_ns"]
                    )
                    and fingerprint(stream, 0, previous["prefix_length"])
                    == previous["prefix"]
                    and fingerprint(stream, max(0, offset - 256), min(offset, 256))
                    == previous["boundary"]
                )
                if not unchanged:
                    offset = 0
                    result.warnings.append("file_rewritten_or_truncated")
                else:
                    dropping = previous.get("dropping_line", False)
            elif not backfill:
                offset = max(
                    0, info.st_size - int(self.config.initial_tail_mb * 1024 * 1024)
                )
                if offset:
                    stream.seek(offset - 1)
                    dropping = stream.read(1) != b"\n"
                    result.warnings.append("initial_history_skipped")
            stream.seek(offset)
            buffer = b""
            position = offset
            end = (
                info.st_size
            )  # Snapshot end: concurrent appends belong to a later run.
            while position < end and budget > 0:
                if time.monotonic() >= deadline:
                    result.warnings.append("timeout")
                    break
                chunk = stream.read(min(65536, budget, end - position))
                if not chunk:
                    break
                result.bytes_read += len(chunk)
                budget -= len(chunk)
                position += len(chunk)
                chunk_start = position - len(chunk)
                consumed = 0
                parts = chunk.split(b"\n")
                for index, part in enumerate(parts):
                    complete = index < len(parts) - 1
                    consumed += len(part) + int(complete)
                    if dropping:
                        if complete:
                            dropping = False
                        continue
                    if len(buffer) + len(part) > self.config.max_line_bytes:
                        result.warnings.append("oversized_line_dropped")
                        buffer = b""
                        dropping = not complete
                        continue
                    buffer += part
                    if complete:
                        try:
                            text, invalid = buffer.decode("utf-8"), False
                        except UnicodeDecodeError:
                            text, invalid = (
                                buffer.decode("utf-8", errors="replace"),
                                True,
                            )
                            result.warnings.append("encoding_replaced")
                        result.lines.append((text.rstrip("\r"), invalid))
                        buffer = b""
                        if len(result.lines) >= max_lines:
                            position = chunk_start + consumed
                            break
                if len(result.lines) >= max_lines:
                    result.warnings.append("record_limit")
                    break
            offset = position - len(buffer)
            if buffer:
                result.warnings.append("incomplete_line_deferred")
            if position < end:
                result.warnings.append("read_limit")
            final = os.fstat(stream.fileno())
            if final.st_size < position:
                raise ScanError("file_truncated_during_read")
            if (
                final.st_mtime_ns != info.st_mtime_ns and final.st_size <= info.st_size
            ) or fingerprint(stream, 0, initial_prefix_length) != initial_prefix:
                raise ScanError("file_rewritten_during_read")
            prefix_length = min(offset, 256)
            result.checkpoint = {
                "path": str(path),
                "device": info.st_dev,
                "inode": info.st_ino,
                "offset": offset,
                "size": final.st_size,
                "mtime_ns": final.st_mtime_ns,
                "prefix_length": prefix_length,
                "prefix": fingerprint(stream, 0, prefix_length),
                "boundary": fingerprint(stream, max(0, offset - 256), min(offset, 256)),
                "dropping_line": dropping,
            }
            return result


class DirectoryCollector(FileCollector):
    def candidates(self, known, deadline):
        root = self.source.path
        root_info = root.lstat()
        if stat.S_ISLNK(root_info.st_mode):
            raise ScanError("symlink_excluded")
        if not stat.S_ISDIR(root_info.st_mode):
            raise ScanError("not_directory")
        pending, candidates, scanned = [root], [], 0
        while pending:
            directory = pending.pop()
            with os.scandir(directory) as entries:
                for entry in entries:
                    scanned += 1
                    if (
                        scanned > self.config.max_scan_entries
                        or time.monotonic() >= deadline
                    ):
                        self.warnings.append("scan_limit")
                        return self.ordered(candidates, known)
                    if entry.is_symlink():
                        continue
                    if entry.is_dir(follow_symlinks=False):
                        if self.source.recursive:
                            pending.append(Path(entry.path))
                        continue
                    info = entry.stat(follow_symlinks=False)
                    if not stat.S_ISREG(info.st_mode) or entry.name.lower().endswith(
                        COMPRESSED
                    ):
                        continue
                    tracked = (info.st_dev, info.st_ino) in known
                    base = ROTATED.sub("", entry.name)
                    rotated = base != entry.name
                    matches = fnmatch.fnmatch(entry.name, self.source.pattern)
                    matches_rotated = (
                        self.source.include_rotated
                        and rotated
                        and fnmatch.fnmatch(base, self.source.pattern)
                    )
                    if tracked or (
                        (matches or matches_rotated)
                        and (self.source.include_rotated or not rotated)
                    ):
                        candidates.append((Path(entry.path), info))
        if not candidates:
            self.warnings.append("no_matching_files")
        return self.ordered(candidates, known)
