import fnmatch
import hashlib
import os
import re
import stat
import time
from collections import deque
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

    directory_mode = False

    def _accept(self, path, info, known):
        tracked = (info.st_dev, info.st_ino) in known
        if self.directory_mode:
            base = ROTATED.sub("", path.name)
            rotated = base != path.name
            return tracked or (
                (
                    fnmatch.fnmatch(path.name, self.source.pattern)
                    or (
                        self.source.include_rotated
                        and rotated
                        and fnmatch.fnmatch(base, self.source.pattern)
                    )
                )
                and (self.source.include_rotated or not rotated)
            )
        active = self.source.path
        return path == active or (
            path.parent == active.parent
            and path.name.startswith(active.name + ".")
            and (tracked or (self.source.include_rotated and ROTATED.search(path.name)))
        )

    def candidates(self, known, deadline):
        root = self.source.path if self.directory_mode else self.source.path.parent
        root_info = root.lstat()
        if stat.S_ISLNK(root_info.st_mode):
            raise ScanError("symlink_excluded")
        if not stat.S_ISDIR(root_info.st_mode):
            raise ScanError("not_directory")
        state = getattr(self, "scan_state", None) or {}
        if state.get("root") != str(root):
            state = {}
        pending = deque(state.get("queue") or [{"directory": ".", "after": None}])
        candidates = {}
        known_cursor = state.get("known_cursor", 0)

        def add(path, info):
            if (
                stat.S_ISREG(info.st_mode)
                and not path.name.lower().endswith(COMPRESSED)
                and self._accept(path, info, known)
            ):
                candidates[str(path)] = (path, info)

        # The exact active file is O(1), independent of its directory discovery cursor.
        if not self.directory_mode:
            try:
                active_info = self.source.path.lstat()
                if stat.S_ISLNK(active_info.st_mode):
                    self.warnings.append("symlink_excluded")
                else:
                    add(self.source.path, active_info)
            except FileNotFoundError:
                self.warnings.append("active_file_missing")

        # Revisit a bounded, rotating window of tracked paths while discovery continues.
        keys = sorted(known)
        revisit = min(
            len(keys), self.config.max_files, self.config.max_scan_entries // 2
        )
        visited = 0
        revisit_deadline = min(
            deadline, time.monotonic() + max(0, deadline - time.monotonic()) * 0.25
        )
        for index in range(revisit):
            if time.monotonic() >= revisit_deadline:
                break
            previous = known[keys[(known_cursor + index) % len(keys)]]
            path = Path(previous["path"])
            try:
                relative = path.relative_to(root)
                if ".." in relative.parts or (
                    not self.source.recursive
                    and self.directory_mode
                    and path.parent != root
                ):
                    continue
                parent = path.parent
                safe = True
                while parent != root:
                    if parent.is_symlink():
                        safe = False
                        break
                    parent = parent.parent
                if safe:
                    add(path, path.lstat())
            except (ValueError, FileNotFoundError):
                pass
            finally:
                visited += 1
        known_cursor = (known_cursor + visited) % max(1, len(keys))
        scanned = visited
        paused = False
        while pending and not paused:
            cursor = pending.popleft()
            relative = Path(cursor["directory"])
            if relative.is_absolute() or ".." in relative.parts:
                raise ScanError("invalid_scan_cursor")
            directory = root / relative
            try:
                if directory.is_symlink():
                    continue
                with os.scandir(directory) as entries:
                    skipping = cursor.get("after") is not None
                    descended = False
                    for entry in entries:
                        if (
                            time.monotonic() >= deadline
                            or scanned >= self.config.max_scan_entries
                        ):
                            self.warnings.append(
                                "scan_timeout"
                                if time.monotonic() >= deadline
                                else "scan_entry_limit"
                            )
                            pending.appendleft(cursor)
                            paused = True
                            break
                        if skipping:
                            if entry.name == cursor["after"]:
                                skipping = False
                            continue
                        # Only update the anchor after an entry is handled.
                        if (
                            entry.is_dir(follow_symlinks=False)
                            and self.directory_mode
                            and self.source.recursive
                        ):
                            child = relative / entry.name
                            if len(child.parts) > 64:
                                self.warnings.append("scan_depth_limit")
                            else:
                                cursor["after"] = entry.name
                                scanned += 1
                                pending.appendleft(cursor)
                                pending.appendleft(
                                    {"directory": str(child), "after": None}
                                )
                                descended = True
                                break
                        elif not entry.is_symlink() and not entry.name.lower().endswith(
                            COMPRESSED
                        ):
                            try:
                                add(Path(entry.path), entry.stat(follow_symlinks=False))
                            except FileNotFoundError:
                                self.warnings.append("file_disappeared_during_scan")
                        cursor["after"] = entry.name
                        scanned += 1
                    if not paused and not descended and skipping:
                        # Anchor was removed between runs; restart this directory safely.
                        cursor["after"] = None
                        pending.appendleft(cursor)
                        self.warnings.append("scan_cursor_reset")
                        paused = True
            except FileNotFoundError:
                self.warnings.append("directory_disappeared_during_scan")
            except PermissionError:
                self.warnings.append("permission_denied")
        self.next_scan_state = {
            "root": str(root),
            "queue": list(pending),
            "known_cursor": known_cursor,
        }
        if not candidates and not pending:
            self.warnings.append(
                "no_matching_files" if self.directory_mode else "active_file_missing"
            )
        return self.ordered(list(candidates.values()), known)

    @staticmethod
    def ordered(candidates, known):
        def priority(pair):
            path, info = pair
            previous = known.get((info.st_dev, info.st_ino))
            if previous is None:
                return (0, "", info.st_mtime_ns, str(path))
            changed = (
                previous["offset"] < info.st_size
                or previous["mtime_ns"] != info.st_mtime_ns
            )
            return (
                1 if changed else 2,
                previous["last_success"],
                info.st_mtime_ns,
                str(path),
            )

        # Give unseen files a first read; rotate backlog by last success, then revisit unchanged files.
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
                chunk = stream.readline(min(65536, budget, end - position))
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
                    if position < end:
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
    directory_mode = True
