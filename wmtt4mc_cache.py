import hashlib
import io
import json
import os
import sqlite3
import struct
import tempfile
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

import numpy as np
import zstandard as zstd


CACHE_FORMAT_VERSION = 1

# --- Compressed .wmtt4mc format -------------------------------------------
#
# Layout (all big-endian):
#   Offset  Size  Description
#   0       4     Magic: b"WMTZ"  (distinguishes compressed from legacy SQLite)
#   4       2     Format version: uint16  (currently 1)
#   6       4     Metadata JSON byte length: uint32
#   10      N     Metadata JSON (UTF-8), N = value from offset 6
#   10+N    rest  Zstandard-compressed SQLite bytes
#
# The original plain-SQLite format begins with b"SQLite format 3\x00" (16 bytes),
# which can never start with b"WMTZ", so detection is unambiguous.
#
# Metadata JSON fields (informational; full truth is inside the SQLite):
#   format_version, source_name, source_size, source_hash,
#   cache_mode, dimension, y_min, y_max
# ---------------------------------------------------------------------------

_COMPRESSED_MAGIC = b"WMTZ"
_COMPRESSED_VERSION = 1
_HEADER_FIXED = struct.Struct(">4sHI")  # magic(4s) + version(H) + meta_len(I)


def _is_compressed_cache(path: str) -> bool:
    """Return True if *path* is a new-style zstd-compressed .wmtt4mc file."""
    try:
        with open(path, "rb") as f:
            return f.read(4) == _COMPRESSED_MAGIC
    except Exception:
        return False


def _compress_cache(sqlite_path: str, out_path: str, outer_metadata: Dict[str, Any]) -> None:
    """Read *sqlite_path*, compress it with zstd, write to *out_path*.

    *outer_metadata* is a small JSON blob stored in the file header for quick
    inspection without decompressing the full payload.
    """
    meta_bytes = json.dumps(outer_metadata, separators=(",", ":"), sort_keys=True).encode("utf-8")
    cctx = zstd.ZstdCompressor(level=3, threads=-1)
    with open(sqlite_path, "rb") as src:
        compressed = cctx.compress(src.read())
    header = _HEADER_FIXED.pack(_COMPRESSED_MAGIC, _COMPRESSED_VERSION, len(meta_bytes))
    tmp = out_path + ".ctmp"
    try:
        with open(tmp, "wb") as dst:
            dst.write(header)
            dst.write(meta_bytes)
            dst.write(compressed)
        os.replace(tmp, out_path)
    except Exception:
        try:
            os.unlink(tmp)
        except Exception:
            pass
        raise


def _decompress_cache_to_temp(path: str) -> Tuple[str, Dict[str, Any]]:
    """Decompress a compressed .wmtt4mc file to a temp SQLite file.

    Returns ``(temp_sqlite_path, outer_metadata_dict)``.

    The caller is responsible for deleting the temp file when finished.
    """
    with open(path, "rb") as f:
        fixed = f.read(_HEADER_FIXED.size)
        magic, version, meta_len = _HEADER_FIXED.unpack(fixed)
        if magic != _COMPRESSED_MAGIC:
            raise ValueError(f"Not a compressed cache: {path}")
        meta_bytes = f.read(meta_len)
        compressed = f.read()

    outer_meta: Dict[str, Any] = {}
    try:
        outer_meta = json.loads(meta_bytes.decode("utf-8"))
    except Exception:
        pass

    dctx = zstd.ZstdDecompressor()
    sqlite_bytes = dctx.decompress(compressed)

    fd, tmp_path = tempfile.mkstemp(suffix=".wmtt4mc.sqlite")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(sqlite_bytes)
    except Exception:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass
        raise

    return tmp_path, outer_meta


def open_cache_sqlite(path: str) -> Tuple[sqlite3.Connection, Optional[str]]:
    """Open a .wmtt4mc cache file as a SQLite connection.

    Handles both old plain-SQLite files and new compressed files transparently.

    Returns ``(connection, temp_path_or_None)``.  If *temp_path* is not None
    the caller MUST delete it after closing the connection::

        conn, tmp = open_cache_sqlite(path)
        try:
            ...
        finally:
            conn.close()
            if tmp:
                try: os.unlink(tmp)
                except Exception: pass
    """
    if _is_compressed_cache(path):
        tmp_path, _ = _decompress_cache_to_temp(path)
        try:
            conn = sqlite3.connect(tmp_path)
            return conn, tmp_path
        except Exception:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass
            raise
    else:
        return sqlite3.connect(path), None


CACHE_MODE_SURFACE = "surface"
CACHE_MODE_ALL_BLOCKS = "all_blocks"
CACHE_MODE_NONE = "none"


@dataclass
class SnapshotInput:
    kind: str
    path: str
    display_name: str
    sort_name: str
    raw_path: Optional[str] = None
    cache_path: Optional[str] = None
    warning: str = ""


def is_cache_file(path: str) -> bool:
    return os.path.isfile(path) and path.lower().endswith(".wmtt4mc")


def is_world_folder(path: str) -> bool:
    return os.path.isdir(path) and os.path.isfile(os.path.join(path, "level.dat"))


# Known dimension suffixes embedded in cache filenames.
# "World_overworld.wmtt4mc" stores the overworld; "World_nether.wmtt4mc" the Nether, etc.
_DIM_SUFFIXES: tuple = ("_overworld", "_nether", "_end")

# Known cache mode suffixes appended after the dimension suffix.
# e.g. World_overworld_surface.wmtt4mc / World_overworld_allblocks.wmtt4mc
_MODE_SUFFIXES: tuple = ("_surface", "_allblocks")

_DIM_ID_TO_SUFFIX: Dict[str, str] = {
    "minecraft:overworld": "_overworld",
    "minecraft:the_nether": "_nether",
    "minecraft:the_end": "_end",
}


def _dim_suffix(dimension: str) -> str:
    """Return the filename suffix for a given Minecraft dimension ID.

    Returns an empty string for unknown/unrecognised dimensions.
    """
    return _DIM_ID_TO_SUFFIX.get(str(dimension).lower().strip(), "")


def snapshot_stem(path: str) -> str:
    """Extract the base filename stem (without extension) and normalize for matching.

    Strips known dimension suffixes so that "World_overworld.wmtt4mc" has the
    same stem as "World.zip".  Normalizes Unicode (NFD -> NFC) for cross-platform
    consistency.

    Examples:
        /path/World_Backup.zip           -> World_Backup
        /path/World_Backup (subfolder)   -> World_Backup
        /path/World_Backup.wmtt4mc       -> World_Backup          (old-style)
        /path/World_Backup_overworld.wmtt4mc -> World_Backup      (new-style)
        /path/World_Backup_nether.wmtt4mc    -> World_Backup
    """
    import unicodedata
    name = os.path.basename(path.rstrip("\\/"))
    if os.path.isdir(path):
        stem = name
    else:
        stem = os.path.splitext(name)[0]
    # Strip known mode suffixes first, then dimension suffixes (case-insensitive)
    low = stem.lower()
    for sfx in _MODE_SUFFIXES:
        if low.endswith(sfx):
            stem = stem[: -len(sfx)]
            low = stem.lower()
            break
    for sfx in _DIM_SUFFIXES:
        if low.endswith(sfx):
            stem = stem[: -len(sfx)]
            break
    return unicodedata.normalize('NFC', stem)


def sidecar_cache_path(source_path: str, dimension: str = "", mode: str = "") -> str:
    """Return the sidecar cache file path for *source_path*.

    When *dimension* is given, the dimension is embedded in the filename so that
    multiple dimension caches can live next to the same backup without
    overwriting each other.  *mode* adds an additional suffix to distinguish
    surface-only from all-blocks caches::

        World.zip + "minecraft:overworld" + "surface"    -> World_overworld_surface.wmtt4mc
        World.zip + "minecraft:overworld" + "all_blocks" -> World_overworld_allblocks.wmtt4mc
        World.zip + "minecraft:the_nether" + "surface"   -> World_nether_surface.wmtt4mc
        World.zip + ""                     + ""          -> World.wmtt4mc  (backwards-compat)
    """
    root = os.path.dirname(source_path.rstrip("\\/"))
    stem = snapshot_stem(source_path).rstrip("\\/")
    dim_sfx = _dim_suffix(dimension) if dimension else ""
    if mode == CACHE_MODE_SURFACE:
        mode_sfx = "_surface"
    elif mode == CACHE_MODE_ALL_BLOCKS:
        mode_sfx = "_allblocks"
    else:
        mode_sfx = ""
    return os.path.join(root, stem + dim_sfx + mode_sfx + ".wmtt4mc")


def _hash_file_sample(path: str, sample_bytes: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        head = f.read(sample_bytes)
        h.update(head)
        try:
            size = os.path.getsize(path)
            if size > sample_bytes:
                f.seek(max(0, size - sample_bytes))
                tail = f.read(sample_bytes)
                h.update(tail)
        except Exception:
            pass
    return h.hexdigest()


def build_source_signature(source_path: str) -> Dict[str, Any]:
    if os.path.isfile(source_path):
        st = os.stat(source_path)
        return {
            "source_kind": "file",
            "source_name": os.path.basename(source_path),
            "source_size": int(st.st_size),
            "source_mtime_ns": int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1_000_000_000))),
            "source_hash": _hash_file_sample(source_path),
        }

    level_dat = os.path.join(source_path, "level.dat")
    if os.path.isfile(level_dat):
        st = os.stat(level_dat)
        return {
            "source_kind": "folder",
            "source_name": os.path.basename(source_path.rstrip("\\/")),
            "source_size": int(st.st_size),
            "source_mtime_ns": int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1_000_000_000))),
            "source_hash": "",
        }

    raise FileNotFoundError(f"Not a supported source path: {source_path}")


def _json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _json_loads(value: str) -> Any:
    return json.loads(value)


class CacheWriter:
    def __init__(self, cache_path: str, metadata: Dict[str, Any]):
        self.cache_path = cache_path
        self.tmp_path = cache_path + ".tmp"
        self._init_metadata = dict(metadata)
        # Remove any stale .tmp left by a previous cancelled/crashed run.
        try:
            if os.path.exists(self.tmp_path):
                os.unlink(self.tmp_path)
        except OSError:
            pass
        self.conn = sqlite3.connect(self.tmp_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self.conn.execute("PRAGMA page_size=4096")
        self.conn.execute(
            "CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        self.conn.execute(
            "CREATE TABLE block_ids (id INTEGER PRIMARY KEY, block_text TEXT NOT NULL UNIQUE)"
        )
        self.conn.execute(
            "CREATE TABLE chunks (cx INTEGER NOT NULL, cz INTEGER NOT NULL, surface_payload BLOB NOT NULL, deep_payload BLOB, PRIMARY KEY (cx, cz))"
        )
        self.conn.execute("CREATE INDEX idx_chunks_cz_cx ON chunks (cz, cx)")
        self._block_to_id: Dict[str, int] = {}
        self._chunk_count = 0

        meta = dict(metadata)
        meta["format_version"] = CACHE_FORMAT_VERSION
        for key, value in meta.items():
            self.conn.execute(
                "INSERT INTO metadata (key, value) VALUES (?, ?)",
                (str(key), _json_dumps(value)),
            )
        self.conn.commit()

    def ensure_block_id(self, block_text: str) -> int:
        block_text = str(block_text)
        existing = self._block_to_id.get(block_text)
        if existing is not None:
            return existing
        cur = self.conn.execute(
            "INSERT INTO block_ids (block_text) VALUES (?)",
            (block_text,),
        )
        block_id = int(cur.lastrowid)
        self._block_to_id[block_text] = block_id
        return block_id

    def write_chunk(self, cx: int, cz: int, surface_payload: bytes, deep_payload: Optional[bytes] = None) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO chunks (cx, cz, surface_payload, deep_payload) VALUES (?, ?, ?, ?)",
            (int(cx), int(cz), sqlite3.Binary(surface_payload), sqlite3.Binary(deep_payload) if deep_payload is not None else None),
        )
        self._chunk_count += 1
        if (self._chunk_count % 32) == 0:
            self.conn.commit()

    def finalize(self, extra_metadata: Optional[Dict[str, Any]] = None) -> None:
        meta = {
            "chunk_count": self._chunk_count,
            "block_count": len(self._block_to_id),
        }
        if extra_metadata:
            meta.update(extra_metadata)
        for key, value in meta.items():
            self.conn.execute(
                "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
                (str(key), _json_dumps(value)),
            )
        self.conn.commit()
        self.conn.close()

        # Build the outer-header metadata (quick inspection without decompressing)
        outer_meta: Dict[str, Any] = {"format_version": CACHE_FORMAT_VERSION}
        for key in ("source_name", "source_size", "source_hash", "cache_mode", "dimension", "y_min", "y_max"):
            if key in self._init_metadata:
                outer_meta[key] = self._init_metadata[key]

        _compress_cache(self.tmp_path, self.cache_path, outer_meta)
        try:
            os.unlink(self.tmp_path)
        except Exception:
            pass


def read_cache_header(cache_path: str) -> Dict[str, Any]:
    conn, tmp = open_cache_sqlite(cache_path)
    try:
        rows = conn.execute("SELECT key, value FROM metadata").fetchall()
        return {str(k): _json_loads(v) for k, v in rows}
    finally:
        conn.close()
        if tmp:
            try:
                os.unlink(tmp)
            except Exception:
                pass


class _CacheHeaderCache:
    """Simple LRU cache for cache file headers. Reduces repeated SQLite opens."""
    def __init__(self, max_size: int = 256):
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.max_size = max_size
        self.hits = 0
        self.misses = 0
    
    def get(self, cache_path: str) -> Optional[Dict[str, Any]]:
        """Get header from cache, or read from file if not cached."""
        if cache_path in self.cache:
            self.hits += 1
            return self.cache[cache_path]
        
        self.misses += 1
        try:
            header = read_cache_header(cache_path)
            # Simple eviction: if cache is full, clear oldest entries
            if len(self.cache) >= self.max_size:
                # Remove half the entries to make room
                to_remove = list(self.cache.keys())[:self.max_size // 2]
                for k in to_remove:
                    del self.cache[k]
            self.cache[cache_path] = header
            return header
        except Exception:
            return None
    
    def stats(self) -> str:
        total = self.hits + self.misses
        if total == 0:
            return "0 accesses"
        hit_rate = (self.hits / total) * 100
        return f"{total} accesses ({hit_rate:.1f}% hit rate)"


_header_cache = _CacheHeaderCache()


def read_cache_header_cached(cache_path: str) -> Optional[Dict[str, Any]]:
    """Read cache header with in-memory caching to reduce I/O."""
    return _header_cache.get(cache_path)


def read_block_lookup(cache_path: str) -> List[str]:
    conn, tmp = open_cache_sqlite(cache_path)
    try:
        rows = conn.execute("SELECT id, block_text FROM block_ids ORDER BY id ASC").fetchall()
        if not rows:
            return [""]
        max_id = max(int(r[0]) for r in rows)
        lookup = [""] * (max_id + 1)
        for block_id, block_text in rows:
            lookup[int(block_id)] = str(block_text)
        return lookup
    finally:
        conn.close()
        if tmp:
            try:
                os.unlink(tmp)
            except Exception:
                pass


def iter_chunk_rows(cache_path: str, min_cx: Optional[int] = None, max_cx: Optional[int] = None, min_cz: Optional[int] = None, max_cz: Optional[int] = None) -> Iterator[Tuple[int, int, bytes, Optional[bytes]]]:
    conn, tmp = open_cache_sqlite(cache_path)
    try:
        clauses: List[str] = []
        params: List[int] = []
        if min_cx is not None:
            clauses.append("cx >= ?")
            params.append(int(min_cx))
        if max_cx is not None:
            clauses.append("cx <= ?")
            params.append(int(max_cx))
        if min_cz is not None:
            clauses.append("cz >= ?")
            params.append(int(min_cz))
        if max_cz is not None:
            clauses.append("cz <= ?")
            params.append(int(max_cz))
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = f"SELECT cx, cz, surface_payload, deep_payload FROM chunks{where} ORDER BY cz ASC, cx ASC"
        # Fetch all rows before closing so temp file cleanup is safe for both
        # old plain-SQLite (no temp) and new compressed (temp must survive iteration).
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()
        if tmp:
            try:
                os.unlink(tmp)
            except Exception:
                pass
    for row in rows:
        yield int(row[0]), int(row[1]), bytes(row[2]), bytes(row[3]) if row[3] is not None else None


def encode_surface_payload(top_id: np.ndarray, top_y: np.ndarray, top_found: np.ndarray, dry_id: np.ndarray, dry_y: np.ndarray, dry_found: np.ndarray) -> bytes:
    bio = io.BytesIO()
    np.savez_compressed(
        bio,
        top_id=top_id,
        top_y=top_y,
        top_found=top_found,
        dry_id=dry_id,
        dry_y=dry_y,
        dry_found=dry_found,
    )
    return bio.getvalue()


def decode_surface_payload(payload: bytes) -> Dict[str, np.ndarray]:
    with np.load(io.BytesIO(payload), allow_pickle=False) as data:
        return {key: data[key] for key in data.files}


def encode_deep_payload(offsets: np.ndarray, seg_top: np.ndarray, seg_bottom: np.ndarray, seg_block_id: np.ndarray) -> bytes:
    """Encode segment data with optimized dtypes for compression.
    
    Uses uint32 for offsets, int16 for Y values (typical MC range -64..320),
    and preserves block ID type. Also applies zlib compression.
    """
    bio = io.BytesIO()
    
    # Optimize dtypes for better compression:
    # - offsets as uint32 (can't be larger)
    # - Y values as int16 (Minecraft Y range fit comfortably)
    opt_offsets = offsets.astype(np.uint32, copy=False)
    opt_seg_top = seg_top.astype(np.int16, copy=False) if seg_top.dtype != np.int16 else seg_top
    opt_seg_bottom = seg_bottom.astype(np.int16, copy=False) if seg_bottom.dtype != np.int16 else seg_bottom
    
    np.savez_compressed(
        bio,
        offsets=opt_offsets,
        seg_top=opt_seg_top,
        seg_bottom=opt_seg_bottom,
        seg_block_id=seg_block_id,  # Keep as-is (usually uint16)
    )
    return bio.getvalue()


def decode_deep_payload(payload: bytes) -> Dict[str, np.ndarray]:
    """Decode segment data from compressed format."""
    with np.load(io.BytesIO(payload), allow_pickle=False) as data:
        result = {key: data[key] for key in data.files}
        # Restore original dtypes if needed for compatibility
        if 'seg_top' in result:
            result['seg_top'] = result['seg_top'].astype(np.int32, copy=False)
        if 'seg_bottom' in result:
            result['seg_bottom'] = result['seg_bottom'].astype(np.int32, copy=False)
        return result


def cache_matches_source(cache_header: Dict[str, Any], source_path: str) -> bool:
    try:
        sig = build_source_signature(source_path)
    except Exception:
        return False
    for key in ("source_kind", "source_name", "source_size", "source_mtime_ns"):
        if cache_header.get(key) != sig.get(key):
            return False
    return True


def discover_raw_snapshot_sources(folder: str) -> List[SnapshotInput]:
    out: List[SnapshotInput] = []
    try:
        names = sorted(os.listdir(folder), key=lambda s: s.lower())
    except Exception:
        return []

    for name in names:
        path = os.path.join(folder, name)
        if os.path.isfile(path) and name.lower().endswith(".zip"):
            out.append(SnapshotInput(kind="zip", path=path, display_name=name, sort_name=name))
        elif is_world_folder(path):
            out.append(SnapshotInput(kind="folder", path=path, display_name=name, sort_name=name))
    return out


def _cache_file_mode_suffix(filename: str) -> str:
    """Return the mode suffix (_surface or _allblocks) present in a cache filename stem, stripping any dim suffix first."""
    stem = os.path.splitext(filename.lower())[0]
    # Strip dim suffix first so mode suffix is at the end
    for dsf in _DIM_SUFFIXES:
        if stem.endswith(dsf):
            stem = stem[: -len(dsf)]
            break
    for msf in _MODE_SUFFIXES:
        if stem.endswith(msf):
            return msf
    return ""


def _cache_file_dim_suffix(filename: str) -> str:
    """Return which dimension suffix (if any) is present in a cache filename stem."""
    stem = os.path.splitext(filename.lower())[0]
    # Strip mode suffix first so dimension suffix is at the end
    for msf in _MODE_SUFFIXES:
        if stem.endswith(msf):
            stem = stem[: -len(msf)]
            break
    for sfx in _DIM_SUFFIXES:
        if stem.endswith(sfx):
            return sfx
    return ""


def _pick_best_cache(
    candidates: List["SnapshotInput"],
    requested_dim: str,
    requested_mode: str = "",
) -> Optional["SnapshotInput"]:
    """Pick the most appropriate cache from multiple candidates for the same base stem.

    Priority:
    1. Exact dimension + exact mode match
    2. Exact dimension + allblocks mode (most capable, works for any Y range)
    3. Exact dimension + surface mode (surface-only)
    4. Exact dimension + no mode suffix (backwards compat)
    5. Old-style cache with no dimension suffix (backwards compat)
    6. Any remaining candidate (last resort)
    """
    req_suffix = _dim_suffix(requested_dim).lower() if requested_dim else ""
    req_mode_sfx = (
        "_surface" if requested_mode == CACHE_MODE_SURFACE
        else "_allblocks" if requested_mode == CACHE_MODE_ALL_BLOCKS
        else ""
    )

    # 1. Exact dim + exact mode
    if req_suffix and req_mode_sfx:
        for c in candidates:
            if _cache_file_dim_suffix(c.display_name) == req_suffix and _cache_file_mode_suffix(c.display_name) == req_mode_sfx:
                return c

    # 2. Exact dim + allblocks (most capable)
    if req_suffix:
        for c in candidates:
            if _cache_file_dim_suffix(c.display_name) == req_suffix and _cache_file_mode_suffix(c.display_name) == "_allblocks":
                return c

    # 3. Exact dim + surface
    if req_suffix:
        for c in candidates:
            if _cache_file_dim_suffix(c.display_name) == req_suffix and _cache_file_mode_suffix(c.display_name) == "_surface":
                return c

    # 4. Exact dim + no mode suffix (backwards compat)
    if req_suffix:
        for c in candidates:
            if _cache_file_dim_suffix(c.display_name) == req_suffix and _cache_file_mode_suffix(c.display_name) == "":
                return c

    # 5. Old-style (no dim suffix, no mode suffix)
    for c in candidates:
        if _cache_file_dim_suffix(c.display_name) == "" and _cache_file_mode_suffix(c.display_name) == "":
            return c

    # 6. Any
    return candidates[0] if candidates else None


def _resolve_cache_item(
    cache_item: "SnapshotInput",
    raw_item: Optional["SnapshotInput"],
) -> "SnapshotInput":
    """Attach source-name metadata to a cache item and optionally link the raw source."""
    try:
        header = read_cache_header_cached(cache_item.path)
        if header is not None:
            if raw_item is not None:
                if cache_matches_source(header, raw_item.path):
                    cache_item.raw_path = raw_item.path
                    cache_item.display_name = header.get("source_name", raw_item.display_name)
                    cache_item.sort_name = cache_item.display_name
                    return cache_item
                else:
                    raw_item.cache_path = cache_item.path
                    raw_item.warning = (
                        f"[STALE CACHE] Cache exists but doesn't match {raw_item.display_name}. "
                        "Using raw source. Rebuild cache to refresh."
                    )
                    return raw_item
            else:
                # Orphaned – no matching raw backup
                cache_item.display_name = header.get("source_name", cache_item.display_name)
                cache_item.sort_name = cache_item.display_name
                return cache_item
        else:
            if raw_item is not None:
                raw_item.cache_path = cache_item.path
                raw_item.warning = (
                    f"[CORRUPT CACHE] Could not read cache for {raw_item.display_name}. "
                    "Using raw source."
                )
                return raw_item
            cache_item.warning = "[ORPHANED/CORRUPT] Cache file cannot be read."
            return cache_item
    except Exception as exc:
        if raw_item is not None:
            raw_item.cache_path = cache_item.path
            raw_item.warning = (
                f"[CORRUPT CACHE] Could not read cache for {raw_item.display_name} "
                f"({type(exc).__name__}). Using raw source."
            )
            return raw_item
        cache_item.warning = f"[CORRUPT CACHE] {type(exc).__name__}"
        return cache_item


def discover_snapshot_inputs(folder: str, dimension: str = "") -> List[SnapshotInput]:
    """Discover and match cache files with backup sources.

    *dimension* (e.g. ``"minecraft:overworld"``) is used to prefer the right
    dimension-specific cache when multiple caches exist for the same backup
    (e.g. ``World_overworld.wmtt4mc`` and ``World_nether.wmtt4mc``).  When
    ``dimension`` is empty any valid cache is accepted and dimension validation
    is deferred to render time.

    Returns a list of :class:`SnapshotInput` objects in sorted order, preferring
    cache files over raw backups.
    """
    raw_inputs = discover_raw_snapshot_sources(folder)
    raw_by_stem: Dict[str, SnapshotInput] = {
        snapshot_stem(item.path).lower(): item for item in raw_inputs
    }

    # Collect ALL .wmtt4mc files, grouped by their base stem so we can handle
    # multiple dimension-specific caches for the same backup.
    cache_candidates_by_stem: Dict[str, List[SnapshotInput]] = {}
    try:
        names = sorted(os.listdir(folder), key=lambda s: s.lower())
    except Exception:
        names = []
    for name in names:
        path = os.path.join(folder, name)
        if not is_cache_file(path):
            continue
        base_stem = snapshot_stem(path).lower()
        item = SnapshotInput(kind="cache", path=path, display_name=name, sort_name=name, cache_path=path)
        cache_candidates_by_stem.setdefault(base_stem, []).append(item)

    # Pick one best cache per stem according to the requested dimension
    cache_by_stem: Dict[str, SnapshotInput] = {
        stem: _pick_best_cache(cands, dimension)
        for stem, cands in cache_candidates_by_stem.items()
        if cands
    }

    resolved: List[SnapshotInput] = []
    for stem in sorted(set(raw_by_stem) | set(cache_by_stem)):
        raw_item = raw_by_stem.get(stem)
        cache_item = cache_by_stem.get(stem)

        if cache_item is not None:
            resolved.append(_resolve_cache_item(cache_item, raw_item))
        elif raw_item is not None:
            resolved.append(raw_item)

    return resolved


def discover_with_diagnostics(folder: str, log_cb: Optional[Callable] = None, dimension: str = "") -> Tuple[List[SnapshotInput], Dict[str, Any]]:
    """Discover snapshots and return detailed diagnostics for logging.
    
    Args:
        folder: Path to search folder
        log_cb: Optional callback(message: str) for logging
        
    Returns:
        Tuple of (snapshots, diagnostics_dict) where diagnostics_dict contains:
        - raw_sources: List of raw backups found
        - cache_files: List of cache files found
        - matches: Count of cache files that matched sources
        - stale_caches: Count of caches that didn't match
        - orphaned_caches: Count of caches without matching sources
        - corrupted_caches: Caches that couldn't be read
    """
    def log(msg):
        if log_cb:
            log_cb(msg)
    
    diag: Dict[str, Any] = {
        "raw_sources": [],
        "cache_files": [],
        "ignored_tmp_files": [],
        "matches": 0,
        "stale_caches": 0,
        "orphaned_caches": 0,
        "corrupted_caches": 0,
        "total": 0,
    }
    
    try:
        names = sorted(os.listdir(folder), key=lambda s: s.lower())
    except Exception as e:
        log(f"[DISCOVERY ERROR] Cannot read folder {folder}: {e}")
        return [], diag
    
    # Discover raw sources
    raw_sources = discover_raw_snapshot_sources(folder)
    log(f"[DISCOVERY] Found {len(raw_sources)} raw backup(s).")
    for src in raw_sources:
        diag["raw_sources"].append(src.display_name)
        log(f"  - {src.display_name} (kind: {src.kind})")
    
    # Discover cache files
    cache_items = []
    for name in names:
        path = os.path.join(folder, name)
        if is_cache_file(path):
            cache_items.append((name, path))
            diag["cache_files"].append(name)
        elif os.path.isfile(path) and name.lower().endswith(".wmtt4mc.tmp"):
            diag["ignored_tmp_files"].append(name)
    log(f"[DISCOVERY] Found {len(cache_items)} cache file(s).")
    for name, path in cache_items:
        log(f"  - {name}")
    if diag["ignored_tmp_files"]:
        log(f"[DISCOVERY] Ignoring {len(diag['ignored_tmp_files'])} temporary cache file(s) (*.wmtt4mc.tmp).")
        for name in diag["ignored_tmp_files"]:
            log(f"  - {name}")
    
    # Run discovery with matching
    results = discover_snapshot_inputs(folder, dimension=dimension)
    diag["total"] = len(results)
    
    # Analyze results
    for snap in results:
        if snap.kind == "cache" and snap.raw_path:
            diag["matches"] += 1
        elif snap.kind == "zip" or snap.kind == "folder":
            if snap.cache_path:
                if "[STALE" in snap.warning:
                    diag["stale_caches"] += 1
                if "[CORRUPT" in snap.warning:
                    diag["corrupted_caches"] += 1
        elif snap.kind == "cache":
            if "[ORPHANED" in snap.warning:
                diag["orphaned_caches"] += 1
    
    # Log summary
    log(f"[DISCOVERY SUMMARY]")
    log(f"  Total items: {diag['total']}")
    log(f"  Matched caches: {diag['matches']}")
    if diag["stale_caches"]:
        log(f"  Stale caches (ignored): {diag['stale_caches']}")
    if diag["orphaned_caches"]:
        log(f"  Orphaned caches (no source): {diag['orphaned_caches']}")
    if diag["corrupted_caches"]:
        log(f"  Corrupted cache files: {diag['corrupted_caches']} (will be ignored)")
    if diag["ignored_tmp_files"]:
        log(f"  Ignored temporary cache files: {len(diag['ignored_tmp_files'])}")
    
    # Log cache I/O performance (if caching is enabled)
    try:
        cache_stats = _header_cache.stats()
        log(f"[CACHE I/O] Header cache: {cache_stats}")
    except Exception:
        pass
    
    # Log any warnings
    for snap in results:
        if snap.warning:
            log(f"[WARNING] {snap.warning}")
    
    return results, diag