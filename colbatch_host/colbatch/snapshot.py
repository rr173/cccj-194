"""Immutable, content-addressed input snapshots.

The profiler is only ever allowed to read a *private, immutable snapshot* of
the input -- never the mutable source path directly.  This module guarantees:

  * the snapshot is created with ``O_CREAT|O_EXCL`` (never overwrites or
    follows a planted temp name) and written with a bounded buffer;
  * a SHA-256 of every copied byte is computed during the single streaming
    copy, so no second full read of the source is needed;
  * if the source file is replaced or modified while the copy is in
    progress, publication is refused and every temp file is removed;
  * the snapshot is made read-only and published atomically (``os.replace``)
    alongside its sidecar metadata;
  * failure, exception or cancellation always cleans up temp files.

Source-change detection combines stat identity (device/inode/size/mtime)
with a content hash of the head and tail of the source, sampled before and
after the copy.  An in-place rewrite that preserves size and inode still
flips a hash; a rename-and-replace flips the inode.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
from dataclasses import dataclass


class SnapshotError(Exception):
    """Snapshot creation or verification failed (I/O or source mutation)."""


@dataclass(frozen=True)
class SnapshotInfo:
    path: str
    sha256: str
    size_bytes: int
    source_identity: dict


def _source_identity(path: str) -> dict:
    st = os.stat(path)
    return {
        "dev": st.st_dev,
        "ino": st.st_ino,
        "size": st.st_size,
        "mtime_ns": st.st_mtime_ns,
    }


def _hash_span(path: str, start: int, length: int, block: int) -> str:
    """Hash up to ``length`` bytes starting at ``start`` (bounded read)."""
    h = hashlib.sha256()
    remaining = length
    with open(path, "rb") as fh:
        fh.seek(start)
        while remaining > 0:
            chunk = fh.read(min(block, remaining))
            if not chunk:
                break
            h.update(chunk)
            remaining -= len(chunk)
    return h.hexdigest()


def _head_tail_hash(path: str, size: int, block: int) -> str:
    """Content fingerprint of head + tail, O(block) memory regardless of size."""
    span = min(size, block)
    head = _hash_span(path, 0, span, block)
    tail_start = max(0, size - span)
    tail = _hash_span(path, tail_start, span, block) if tail_start > 0 else ""
    return hashlib.sha256(f"{size}:{head}:{tail}".encode("ascii")).hexdigest()


def create_snapshot(
    source_path: str,
    dest_path: str,
    temp_dir: str,
    block_bytes: int = 1 << 20,
    cancel_flag=None,
) -> SnapshotInfo:
    """Copy ``source_path`` to an immutable snapshot at ``dest_path``.

    Raises :class:`SnapshotError` if the source changes during the copy or
    any I/O step fails; nothing is published in that case and all temp files
    are removed.
    """
    if block_bytes < 4096:
        raise SnapshotError("block_bytes must be >= 4096")
    os.makedirs(temp_dir, exist_ok=True)
    os.makedirs(os.path.dirname(os.path.abspath(dest_path)), exist_ok=True)

    before_stat = _source_identity(source_path)
    before_ht = _head_tail_hash(source_path, before_stat["size"], block_bytes)

    # Exclusive creation: a pre-existing/planted temp name fails loudly.
    fd, tmp_path = tempfile.mkstemp(
        prefix=".snap-", suffix=".tmp", dir=temp_dir
    )
    published = False
    hasher = hashlib.sha256()
    total = 0
    try:
        with os.fdopen(fd, "wb", closefd=True) as out:
            with open(source_path, "rb") as src:
                while True:
                    if cancel_flag is not None and cancel_flag():
                        raise SnapshotError("snapshot cancelled")
                    chunk = src.read(block_bytes)
                    if not chunk:
                        break
                    out.write(chunk)
                    hasher.update(chunk)
                    total += len(chunk)
            out.flush()
            os.fsync(out.fileno())

        # Re-check identity and content *before* publishing.  This is the
        # key guarantee: hashing only after the report would not prove that
        # the scanned bytes matched one stable version of the source.
        after_stat = _source_identity(source_path)
        after_ht = _head_tail_hash(source_path, after_stat["size"], block_bytes)
        if (
            after_stat["dev"] != before_stat["dev"]
            or after_stat["ino"] != before_stat["ino"]
        ):
            raise SnapshotError(
                "source file was replaced (different device/inode) during snapshot copy"
            )
        if after_stat["size"] != before_stat["size"]:
            raise SnapshotError("source file size changed during snapshot copy")
        if after_ht != before_ht:
            raise SnapshotError(
                "source file content changed during snapshot copy (head/tail hash mismatch)"
            )
        if total != before_stat["size"]:
            raise SnapshotError(
                f"copied {total} bytes but source size is {before_stat['size']}"
            )

        os.chmod(tmp_path, 0o444)  # read-only immutable snapshot
        os.replace(tmp_path, dest_path)
        published = True
        return SnapshotInfo(
            path=dest_path,
            sha256=hasher.hexdigest(),
            size_bytes=total,
            source_identity=before_stat,
        )
    finally:
        if not published and os.path.exists(tmp_path):
            # Restore writable bit so unlink never fails on a chmod'ed file.
            try:
                os.chmod(tmp_path, 0o600)
            except OSError:
                pass
            try:
                os.unlink(tmp_path)
            except OSError:
                pass


def verify_snapshot(path: str, expected_sha256: str, block_bytes: int = 1 << 20) -> int:
    """Re-hash a published snapshot and compare to the bound digest.

    Returns the file size on match, raises :class:`SnapshotError` on any
    mismatch or I/O error.  Used by ``run`` before opening the snapshot and
    after the scan to detect corruption/deletion.
    """
    h = hashlib.sha256()
    try:
        with open(path, "rb") as fh:
            size = 0
            while True:
                chunk = fh.read(block_bytes)
                if not chunk:
                    break
                h.update(chunk)
                size += len(chunk)
    except OSError as exc:
        raise SnapshotError(f"snapshot unreadable: {exc}") from exc
    if h.hexdigest() != expected_sha256:
        raise SnapshotError(
            f"snapshot digest mismatch: expected {expected_sha256}, got {h.hexdigest()}"
        )
    return size
