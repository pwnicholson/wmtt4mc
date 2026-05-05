# === Standard library imports ===
import os
import re
import sys
import io
import time
import zipfile
import shutil
import tempfile
import traceback
import threading
import multiprocessing
import json
import hashlib
from collections import Counter, deque
from datetime import datetime
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from pathlib import Path

# === Third-party imports ===
import numpy as np
from PIL import Image
from dataclasses import dataclass, fields
from typing import Optional, Tuple, Dict, List, Any, Callable, Union
try:
    import amulet
except Exception:
    amulet = None

# === GUI imports ===
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import queue

from wmtt4mc_cache import (
    CACHE_MODE_ALL_BLOCKS,
    CACHE_MODE_NONE,
    CACHE_MODE_SURFACE,
    CacheWriter,
    SnapshotInput,
    build_source_signature,
    decode_deep_payload,
    decode_surface_payload,
    discover_raw_snapshot_sources,
    discover_snapshot_inputs,
    discover_with_diagnostics,
    encode_deep_payload,
    encode_surface_payload,
    is_cache_file,
    is_world_folder,
    iter_chunk_rows,
    read_block_lookup,
    read_cache_header,
    sidecar_cache_path,
    snapshot_stem,
)

# === Palette stubs (prevent NameError) ===
WOOD_MATERIAL_PALETTE = {}
WOOL_COLOR_PALETTE = {}
TERRACOTTA_COLOR_PALETTE = {}
CONCRETE_COLOR_PALETTE = {}
CONCRETE_POWDER_COLOR_PALETTE = {}
CARPET_COLOR_PALETTE = {}
STAINED_GLASS_COLOR_PALETTE = {}
STAINED_GLASS_PANE_COLOR_PALETTE = {}

# Track active process pools so UI stop/close can forcefully terminate workers.
_ACTIVE_PROCESS_POOLS: List[Any] = []
_ACTIVE_PROCESS_POOLS_LOCK = threading.Lock()


def _register_process_pool(pool: Any) -> None:
    with _ACTIVE_PROCESS_POOLS_LOCK:
        if pool not in _ACTIVE_PROCESS_POOLS:
            _ACTIVE_PROCESS_POOLS.append(pool)


def _unregister_process_pool(pool: Any) -> None:
    with _ACTIVE_PROCESS_POOLS_LOCK:
        try:
            _ACTIVE_PROCESS_POOLS.remove(pool)
        except ValueError:
            pass


def _force_stop_registered_pools(log_cb: Optional[Callable[[str], None]] = None) -> None:
    with _ACTIVE_PROCESS_POOLS_LOCK:
        pools = list(_ACTIVE_PROCESS_POOLS)
    for pool in pools:
        try:
            pool.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass
        try:
            procs = getattr(pool, "_processes", None)
            if isinstance(procs, dict):
                for _pid, proc in list(procs.items()):
                    try:
                        if proc is not None and proc.is_alive():
                            proc.terminate()
                    except Exception:
                        pass
                for _pid, proc in list(procs.items()):
                    try:
                        if proc is not None and proc.is_alive() and hasattr(proc, "kill"):
                            proc.kill()
                    except Exception:
                        pass
        except Exception:
            pass
    if log_cb:
        try:
            log_cb(f"[POOL] Forced stop requested for {len(pools)} active process pool(s).")
        except Exception:
            pass


def _raw_block_id(block: Any) -> str:
    """Return a stable raw block identifier string for palette matching/debug logs."""
    if block is None:
        return "minecraft:air"

    try:
        nn = getattr(block, "namespaced_name", None)
        props = None
        for attr in ("properties", "states", "state"):
            cand = getattr(block, attr, None)
            if isinstance(cand, dict):
                props = cand
                break

        if nn:
            base = str(nn)
            if props:
                pairs = []
                for k, v in sorted(props.items(), key=lambda kv: str(kv[0])):
                    vv = getattr(v, "value", v)
                    pairs.append(f'{k}="{vv}"')
                return f"{base}[{','.join(pairs)}]"
            return base
    except Exception:
        pass

    return str(block)


def _config_path() -> str:
    return os.path.join(_app_dir(), "wmtt4mc_settings.json")


def _load_config(path: Optional[str] = None) -> dict:
    path = path or _config_path()
    try:
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
    except Exception:
        pass
    return {}


def _save_config(cfg: dict, path: Optional[str] = None) -> None:
    path = path or _config_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2)
        os.replace(tmp, path)
    except Exception:
        pass


class ScrollableFrame(ttk.Frame):
    """A scrollable container that supports mouse wheel and dynamic content height."""

    def __init__(self, parent, *args, **kwargs):
        super().__init__(parent, *args, **kwargs)
        self.canvas = tk.Canvas(self, highlightthickness=0, borderwidth=0)
        self.vsb = ttk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=self.vsb.set)

        self.vsb.pack(side="right", fill="y")
        self.canvas.pack(side="left", fill="both", expand=True)

        self.interior = ttk.Frame(self.canvas)
        self._window_id = self.canvas.create_window((0, 0), window=self.interior, anchor="nw")

        self.interior.bind("<Configure>", self._on_interior_configure)
        self.canvas.bind("<Configure>", self._on_canvas_configure)

        self.canvas.bind("<Enter>", self._bind_mousewheel)
        self.canvas.bind("<Leave>", self._unbind_mousewheel)
        self.interior.bind("<Enter>", self._bind_mousewheel)
        self.interior.bind("<Leave>", self._unbind_mousewheel)

    def _on_interior_configure(self, _event=None):
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

    def _on_canvas_configure(self, event):
        self.canvas.itemconfigure(self._window_id, width=event.width)

    def _bind_mousewheel(self, _event=None):
        self.canvas.bind_all("<MouseWheel>", self._on_mousewheel)
        self.canvas.bind_all("<Button-4>", self._on_mousewheel)
        self.canvas.bind_all("<Button-5>", self._on_mousewheel)

    def _unbind_mousewheel(self, _event=None):
        self.canvas.unbind_all("<MouseWheel>")
        self.canvas.unbind_all("<Button-4>")
        self.canvas.unbind_all("<Button-5>")

    def _on_mousewheel(self, event):
        if getattr(event, "delta", 0):
            step = -1 if event.delta > 0 else 1
        elif getattr(event, "num", 0) == 4:
            step = -1
        else:
            step = 1
        self.canvas.yview_scroll(step, "units")


# =============================================================================
# Palette editor — shared helpers, widget classes, and state
# =============================================================================

_PalRGB = Tuple[int, int, int]


def _pal_clamp(x: float, lo: int = 0, hi: int = 255) -> int:
    return int(max(lo, min(hi, round(x))))


def _pal_rgb_to_hex(rgb: _PalRGB) -> str:
    r, g, b = rgb
    return f"#{r:02x}{g:02x}{b:02x}"


def _pal_hex_to_rgb(s: str) -> Optional[_PalRGB]:
    s = s.strip().lstrip("#")
    if len(s) == 3:
        s = "".join(c * 2 for c in s)
    if len(s) != 6:
        return None
    try:
        return (int(s[0:2], 16), int(s[2:4], 16), int(s[4:6], 16))
    except Exception:
        return None


def _pal_hsv_to_rgb(h: float, s: float, v: float) -> _PalRGB:
    """h in [0, 360), s/v in [0, 1]."""
    h = h % 360.0
    s = max(0.0, min(1.0, s))
    v = max(0.0, min(1.0, v))
    c = v * s
    x = c * (1.0 - abs(((h / 60.0) % 2) - 1.0))
    m = v - c
    if h < 60:
        rp, gp, bp = c, x, 0.0
    elif h < 120:
        rp, gp, bp = x, c, 0.0
    elif h < 180:
        rp, gp, bp = 0.0, c, x
    elif h < 240:
        rp, gp, bp = 0.0, x, c
    elif h < 300:
        rp, gp, bp = x, 0.0, c
    else:
        rp, gp, bp = c, 0.0, x
    return (_pal_clamp((rp + m) * 255), _pal_clamp((gp + m) * 255), _pal_clamp((bp + m) * 255))


def _pal_rgb_to_hsv(rgb: _PalRGB) -> Tuple[float, float, float]:
    r, g, b = (c / 255.0 for c in rgb)
    mx = max(r, g, b)
    mn = min(r, g, b)
    d = mx - mn
    if d == 0:
        h = 0.0
    elif mx == r:
        h = 60.0 * (((g - b) / d) % 6)
    elif mx == g:
        h = 60.0 * (((b - r) / d) + 2)
    else:
        h = 60.0 * (((r - g) / d) + 4)
    s = 0.0 if mx == 0 else d / mx
    return (h, s, mx)


def _pal_parse_rgb(v: Any) -> Optional[_PalRGB]:
    """Accept [r,g,b], (r,g,b), {'r':..,'g':..,'b':..}, 'r,g,b', '#RRGGBB'."""
    try:
        if isinstance(v, (list, tuple)) and len(v) == 3:
            return (_pal_clamp(v[0]), _pal_clamp(v[1]), _pal_clamp(v[2]))
        if isinstance(v, dict) and all(k in v for k in ("r", "g", "b")):
            return (_pal_clamp(v["r"]), _pal_clamp(v["g"]), _pal_clamp(v["b"]))
        if isinstance(v, str):
            s = v.strip()
            if "," in s:
                p = [x.strip() for x in s.split(",")]
                if len(p) == 3:
                    return (_pal_clamp(int(p[0])), _pal_clamp(int(p[1])), _pal_clamp(int(p[2])))
            return _pal_hex_to_rgb(s)
    except Exception:
        pass
    return None


def _pal_derive_group(block_id: str) -> str:
    """Heuristic family grouping (e.g. 'oak_planks' and 'oak_log' -> 'oak')."""
    try:
        name = block_id.split(":", 1)[1]
    except Exception:
        name = block_id
    parts = [p for p in name.split("_") if p]
    if not parts:
        return "(other)"
    if parts[0] in {"light", "dark"} and len(parts) >= 2:
        return f"{parts[0]}_{parts[1]}"
    if parts[0] in {"red", "blue", "green", "black", "white", "gray", "grey", "brown",
                    "orange", "yellow", "purple", "pink", "cyan", "magenta", "lime"}:
        if len(parts) >= 2 and parts[1] in {"mushroom", "nether", "terracotta", "concrete",
                                             "wool", "glass", "stained"}:
            return f"{parts[0]}_{parts[1]}"
        return parts[0]
    if len(parts) >= 2 and parts[1] in {"planks", "log", "wood", "leaves", "sapling", "slab",
                                        "stairs", "fence", "gate", "door", "trapdoor",
                                        "button", "pressure", "sign", "wall", "banner"}:
        return parts[0]
    if len(parts) >= 3 and parts[1] == "mushroom" and parts[2] == "block":
        return f"{parts[0]}_mushroom"
    return parts[0]


def _pal_normalize_obj(obj: Dict[str, Any]) -> Dict[str, _PalRGB]:
    out: Dict[str, _PalRGB] = {}
    for k, v in obj.items():
        rgb = _pal_parse_rgb(v)
        if rgb is not None:
            out[str(k)] = rgb
    return out


def _pal_load_file(path: str) -> Tuple[Dict[str, Any], Dict[str, _PalRGB], set]:
    """Load palette.json.  Supports WMTT4MC rgb_overrides schema and legacy flat dicts."""
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    if not isinstance(raw, dict):
        raise ValueError("Palette JSON must be an object.")
    if "rgb_overrides" in raw and isinstance(raw["rgb_overrides"], dict):
        palette = _pal_normalize_obj(raw["rgb_overrides"])
        transparent = set(t for t in raw.get("transparent_blocks", []) if isinstance(t, str))
        return raw, palette, transparent
    # Legacy flat mapping
    palette = _pal_normalize_obj(raw)
    return {"schema_version": 1, "rgb_overrides": raw}, palette, set()


def _pal_write_file(path: str, raw_obj: Dict[str, Any], palette: Dict[str, _PalRGB], transparent: set) -> None:
    """Atomically save palette back; creates a timestamped .bak first."""
    import datetime as _dt
    if os.path.exists(path):
        ts = _dt.datetime.now().strftime("%Y%m%d-%H%M%S")
        bak = f"{path}.{ts}.bak"
        try:
            with open(path, "rb") as src, open(bak, "wb") as dst:
                dst.write(src.read())
        except Exception:
            pass
    out = dict(raw_obj) if isinstance(raw_obj, dict) else {}
    out.setdefault("schema_version", 1)
    out["rgb_overrides"] = {k: list(v) for k, v in sorted(palette.items())}
    if transparent:
        out["transparent_blocks"] = sorted(transparent)
    else:
        out.pop("transparent_blocks", None)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        json.dump(out, f, indent=2, sort_keys=True)
        f.write("\n")
    os.replace(tmp, path)


# ---------------------------------------------------------------------------
# Texture-pack color extraction helpers
# ---------------------------------------------------------------------------

_BLOCK_TEXTURE_FACE_SUFFIXES = (
    "_top", "_bottom", "_side", "_front", "_back",
    "_inner", "_outer", "_overlay", "_carried",
    "_still", "_flowing",
)
_BLOCK_TEXTURE_NUMERIC_RE = re.compile(r"_\d+$")


def _pal_strip_texture_stem(stem: str) -> str:
    """Remove face/animation suffixes from a texture filename stem."""
    for sfx in _BLOCK_TEXTURE_FACE_SUFFIXES:
        if stem.endswith(sfx):
            stem = stem[:-len(sfx)]
    stem = _BLOCK_TEXTURE_NUMERIC_RE.sub("", stem)
    return stem


def _pal_is_block_texture_path(path: str) -> bool:
    """Return True if the path points to a block texture in a Java or Bedrock pack."""
    parts = path.lower().replace("\\", "/").split("/")
    # Java: assets/*/textures/block[s]/*.png
    if len(parts) >= 4 and parts[0] == "assets":
        try:
            ti = parts.index("textures")
            if ti + 1 < len(parts) and parts[ti + 1] in ("block", "blocks"):
                return True
        except ValueError:
            pass
    # Bedrock: textures/block[s]/**/*.png
    if len(parts) >= 3 and parts[0] == "textures" and parts[1] in ("block", "blocks"):
        return True
    return False


def _pal_avg_texture_color(img: "Image.Image") -> Optional[_PalRGB]:
    """Return the alpha-weighted average color of a texture image, or None if fully transparent."""
    rgba = img.convert("RGBA")
    data = np.array(rgba, dtype=np.float32)
    weights = np.where(data[:, :, 3] >= 16.0, data[:, :, 3], 0.0)
    total = float(weights.sum())
    if total < 1.0:
        return None
    r = float((data[:, :, 0] * weights).sum() / total)
    g = float((data[:, :, 1] * weights).sum() / total)
    b = float((data[:, :, 2] * weights).sum() / total)
    return (_pal_clamp(int(round(r))), _pal_clamp(int(round(g))), _pal_clamp(int(round(b))))


def _pal_extract_texture_pack(pack_path: str) -> Tuple[Dict[str, _PalRGB], List[str]]:
    """Extract average block colors from a Java or Bedrock texture/resource pack.

    *pack_path* may be a .zip file or an extracted directory.
    Multiple face textures for the same block stem are averaged together.
    Returns ``(palette_dict, warnings)`` where palette_dict maps
    ``"minecraft:block_id"`` → RGB tuple.
    """
    groups: Dict[str, List[_PalRGB]] = {}
    warnings: List[str] = []

    def _process(name: str, data: bytes) -> None:
        stem = os.path.splitext(os.path.basename(name))[0].lower()
        block_id = f"minecraft:{_pal_strip_texture_stem(stem)}"
        try:
            _prev = Image.MAX_IMAGE_PIXELS
            Image.MAX_IMAGE_PIXELS = None
            try:
                img = Image.open(io.BytesIO(data))
                img.load()
            finally:
                Image.MAX_IMAGE_PIXELS = _prev
            # Animated textures are tall strips (h = n×w); use only first frame.
            w, h = img.size
            if w > 0 and h > w and h % w == 0:
                img = img.crop((0, 0, w, w))
            color = _pal_avg_texture_color(img)
            if color is not None:
                groups.setdefault(block_id, []).append(color)
        except Exception as exc:
            warnings.append(f"{os.path.basename(name)}: {exc}")

    if os.path.isdir(pack_path):
        for root, _dirs, files in os.walk(pack_path):
            rel = os.path.relpath(root, pack_path).replace("\\", "/")
            for fn in files:
                if os.path.splitext(fn)[1].lower() not in (".png", ".tga"):
                    continue
                if _pal_is_block_texture_path(rel + "/" + fn):
                    try:
                        with open(os.path.join(root, fn), "rb") as f:
                            _process(fn, f.read())
                    except Exception as exc:
                        warnings.append(f"{fn}: {exc}")
    else:
        if not zipfile.is_zipfile(pack_path):
            raise ValueError(f"Not a valid zip file: {pack_path}")
        with zipfile.ZipFile(pack_path, "r") as z:
            for name in z.namelist():
                if os.path.splitext(name)[1].lower() not in (".png", ".tga"):
                    continue
                if _pal_is_block_texture_path(name):
                    try:
                        _process(name, z.read(name))
                    except Exception as exc:
                        warnings.append(f"{os.path.basename(name)}: {exc}")

    def _avg(cols: List[_PalRGB]) -> _PalRGB:
        r = int(round(sum(c[0] for c in cols) / len(cols)))
        g = int(round(sum(c[1] for c in cols) / len(cols)))
        b = int(round(sum(c[2] for c in cols) / len(cols)))
        return (_pal_clamp(r), _pal_clamp(g), _pal_clamp(b))

    palette = {bid: _avg(cols) for bid, cols in sorted(groups.items())}
    return palette, warnings


@dataclass
class _PaletteEditorState:
    path: Optional[str] = None
    palette: Dict[str, _PalRGB] = None       # type: ignore[assignment]
    raw_obj: Dict[str, Any] = None           # type: ignore[assignment]
    transparent: set = None                  # type: ignore[assignment]
    keys_sorted: List[str] = None            # type: ignore[assignment]
    display_keys: List[str] = None           # type: ignore[assignment]
    display_to_key: Dict[str, str] = None    # type: ignore[assignment]
    unsaved_changes: bool = False

    def __post_init__(self):
        if self.palette is None:
            self.palette = {}
        if self.raw_obj is None:
            self.raw_obj = {}
        if self.transparent is None:
            self.transparent = set()
        if self.keys_sorted is None:
            self.keys_sorted = []
        if self.display_keys is None:
            self.display_keys = []
        if self.display_to_key is None:
            self.display_to_key = {}


class _HSVPicker(ttk.Frame):
    """Hue bar (vertical) + SV square color picker."""

    SV = 200   # SV square side in pixels
    HW = 20    # hue bar width in pixels

    def __init__(self, parent, on_change: Callable):
        super().__init__(parent)
        self._on_change = on_change
        self._h = 0.0
        self._s = 1.0
        self._v = 1.0

        self._sv_canvas = tk.Canvas(self, width=self.SV, height=self.SV,
                                    highlightthickness=1, highlightbackground="#999")
        self._hue_canvas = tk.Canvas(self, width=self.HW, height=self.SV,
                                     highlightthickness=1, highlightbackground="#999")

        self._sv_canvas.grid(row=0, column=0, padx=(0, 8))
        self._hue_canvas.grid(row=0, column=1)

        self._sv_img = tk.PhotoImage(width=self.SV, height=self.SV)
        self._hue_img = tk.PhotoImage(width=self.HW, height=self.SV)
        self._sv_canvas.create_image(0, 0, image=self._sv_img, anchor="nw")
        self._hue_canvas.create_image(0, 0, image=self._hue_img, anchor="nw")

        self._sv_cursor = self._sv_canvas.create_oval(0, 0, 1, 1, outline="white", width=2)
        self._hue_cursor = self._hue_canvas.create_rectangle(0, 0, self.HW, 1,
                                                              outline="white", width=2)
        self._draw_hue()
        self._redraw_sv()
        self._update_cursors()

        self._sv_canvas.bind("<Button-1>", self._sv_click)
        self._sv_canvas.bind("<B1-Motion>", self._sv_click)
        self._hue_canvas.bind("<Button-1>", self._hue_click)
        self._hue_canvas.bind("<B1-Motion>", self._hue_click)

    def set_rgb(self, rgb: _PalRGB) -> None:
        self._h, self._s, self._v = _pal_rgb_to_hsv(rgb)
        self._redraw_sv()
        self._update_cursors()

    def get_rgb(self) -> _PalRGB:
        return _pal_hsv_to_rgb(self._h, self._s, self._v)

    def _draw_hue(self) -> None:
        n = self.SV
        for y in range(n):
            h = 360.0 * y / max(1, n - 1)
            self._hue_img.put(_pal_rgb_to_hex(_pal_hsv_to_rgb(h, 1.0, 1.0)),
                               to=(0, y, self.HW, y + 1))

    def _redraw_sv(self) -> None:
        n = self.SV
        for y in range(n):
            v = 1.0 - y / max(1, n - 1)
            row = [_pal_rgb_to_hex(_pal_hsv_to_rgb(self._h, x / max(1, n - 1), v))
                   for x in range(n)]
            self._sv_img.put("{" + " ".join(row) + "}", to=(0, y))

    def _update_cursors(self) -> None:
        sx = self._s * (self.SV - 1)
        sy = (1.0 - self._v) * (self.SV - 1)
        r = 7
        self._sv_canvas.coords(self._sv_cursor, sx - r, sy - r, sx + r, sy + r)
        hy = (self._h % 360.0) / 360.0 * (self.SV - 1)
        self._hue_canvas.coords(self._hue_cursor, 0, hy - 4, self.HW, hy + 4)

    def _sv_click(self, evt) -> None:
        self._s = max(0.0, min(1.0, evt.x / (self.SV - 1)))
        self._v = max(0.0, min(1.0, 1.0 - evt.y / (self.SV - 1)))
        self._update_cursors()
        self._on_change(self.get_rgb())

    def _hue_click(self, evt) -> None:
        self._h = 360.0 * max(0, min(self.SV - 1, evt.y)) / (self.SV - 1)
        self._redraw_sv()
        self._update_cursors()
        self._on_change(self.get_rgb())


class _PaletteCopyDialog(tk.Toplevel):
    """Modal dialog to copy the current color to other blocks."""

    def __init__(self, parent: tk.Misc, all_keys: List[str],
                 current_key: str, current_transparent: bool):
        super().__init__(parent)
        self.title("Copy color to other blocks")
        self.geometry("680x520")
        self.minsize(540, 400)
        self.transient(parent)
        self.grab_set()
        self.resizable(True, True)

        self.result_keys: List[str] = []
        self.result_copy_transparent = False
        self._all_keys = all_keys
        self._current_transparent = current_transparent
        self._vars: Dict[str, tk.BooleanVar] = {
            k: tk.BooleanVar(value=(k == current_key)) for k in all_keys
        }
        self._copy_transparent_var = tk.BooleanVar(value=False)
        self._filter_var = tk.StringVar()

        # Filter row
        top = ttk.Frame(self)
        top.pack(fill="x", padx=10, pady=(10, 4))
        ttk.Label(top, text="Filter:").pack(side="left")
        ent = ttk.Entry(top, textvariable=self._filter_var)
        ent.pack(side="left", fill="x", expand=True, padx=(6, 0))
        ent.bind("<KeyRelease>", lambda _e: self._rebuild())

        # Scrollable checkbox area
        self._canvas = tk.Canvas(self, highlightthickness=0)
        vsb = ttk.Scrollbar(self, orient="vertical", command=self._canvas.yview)
        self._canvas.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")
        self._canvas.pack(fill="both", expand=True, padx=10, pady=4)
        self._inner = ttk.Frame(self._canvas)
        self._win = self._canvas.create_window(0, 0, window=self._inner, anchor="nw")
        self._inner.bind("<Configure>",
                         lambda _e: self._canvas.configure(
                             scrollregion=self._canvas.bbox("all")))
        self._canvas.bind("<Configure>",
                          lambda e: self._canvas.itemconfigure(self._win, width=e.width))
        self._canvas.bind("<Enter>",
                          lambda _e: self._canvas.bind_all("<MouseWheel>", self._on_mw))
        self._canvas.bind("<Leave>",
                          lambda _e: self._canvas.unbind_all("<MouseWheel>"))

        # Bottom buttons
        btns = ttk.Frame(self)
        btns.pack(fill="x", padx=10, pady=(4, 10))
        ttk.Button(btns, text="Select all (filtered)",
                   command=self._select_all).pack(side="left")
        ttk.Button(btns, text="Clear all",
                   command=self._clear_all).pack(side="left", padx=(8, 0))
        ttk.Checkbutton(btns, text="Also copy Transparent setting",
                        variable=self._copy_transparent_var).pack(side="left", padx=(14, 0))
        ttk.Button(btns, text="Cancel", command=self._cancel).pack(side="right")
        ttk.Button(btns, text="Apply", command=self._apply).pack(side="right", padx=(0, 8))

        self._rebuild()
        ent.focus_set()

    def _on_mw(self, evt) -> None:
        step = -1 if getattr(evt, "delta", 0) > 0 else 1
        self._canvas.yview_scroll(step, "units")

    def _rebuild(self) -> None:
        for w in self._inner.winfo_children():
            w.destroy()
        flt = self._filter_var.get().strip().lower()
        keys = [k for k in self._all_keys if not flt or flt in k.lower()]
        cols = 2
        for i, k in enumerate(keys):
            ttk.Checkbutton(self._inner, text=k, variable=self._vars[k]).grid(
                row=i // cols, column=i % cols, sticky="w", padx=6, pady=2)
        for c in range(cols):
            self._inner.grid_columnconfigure(c, weight=1)

    def _select_all(self) -> None:
        flt = self._filter_var.get().strip().lower()
        for k in self._all_keys:
            if not flt or flt in k.lower():
                self._vars[k].set(True)

    def _clear_all(self) -> None:
        for v in self._vars.values():
            v.set(False)

    def _apply(self) -> None:
        self.result_keys = [k for k in self._all_keys if self._vars[k].get()]
        self.result_copy_transparent = self._copy_transparent_var.get()
        self.destroy()

    def _cancel(self) -> None:
        self.destroy()


class _PaletteConflictDialog(tk.Toplevel):
    """Show color conflicts between the current and an incoming palette.

    Presents a scrollable table with Radiobutton rows so the user can pick
    "current" or "new" color for each conflicting block ID.

    Result: ``.resolutions`` — dict mapping block_id → resolved _PalRGB,
    or ``None`` if the user cancelled.
    """

    def __init__(self, parent: tk.Misc,
                 conflicts: List[Tuple[str, _PalRGB, _PalRGB]]):
        super().__init__(parent)
        self.title(f"Resolve {len(conflicts)} Color Conflict(s)")
        self.geometry("860x520")
        self.minsize(620, 360)
        self.transient(parent)
        self.grab_set()
        self.resizable(True, True)

        self.resolutions: Optional[Dict[str, _PalRGB]] = None
        self._conflicts = conflicts
        self._choices: Dict[str, tk.StringVar] = {
            bid: tk.StringVar(value="current") for bid, _, _ in conflicts
        }

        ttk.Label(
            self,
            text=(
                "The following block IDs have different colors in the two palettes.\n"
                "Choose which color to keep for each, then click Apply."
            ),
            justify="left",
        ).pack(padx=14, pady=(10, 4), anchor="w")

        gbar = ttk.Frame(self)
        gbar.pack(fill="x", padx=14, pady=(0, 4))
        ttk.Button(gbar, text="Keep all current",
                   command=lambda: self._set_all("current")).pack(side="left")
        ttk.Button(gbar, text="Use all new",
                   command=lambda: self._set_all("new")).pack(side="left", padx=(8, 0))

        canvas = tk.Canvas(self, highlightthickness=0)
        vsb = ttk.Scrollbar(self, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")
        canvas.pack(fill="both", expand=True, padx=14, pady=4)
        inner = ttk.Frame(canvas)
        _win = canvas.create_window(0, 0, window=inner, anchor="nw")
        inner.bind("<Configure>",
                   lambda _e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.bind("<Configure>",
                    lambda e: canvas.itemconfigure(_win, width=e.width))
        canvas.bind("<Enter>",
                    lambda _e: canvas.bind_all("<MouseWheel>", self._on_mw))
        canvas.bind("<Leave>",
                    lambda _e: canvas.unbind_all("<MouseWheel>"))
        self._canvas = canvas

        for col, text in enumerate(("Block ID", "Current color", "New color", "Keep")):
            ttk.Label(inner, text=text, font=("", 9, "bold")).grid(
                row=0, column=col, sticky="w", padx=6, pady=(2, 4))

        for row_i, (bid, cur, new) in enumerate(conflicts, start=1):
            cur_hex = _pal_rgb_to_hex(cur)
            new_hex = _pal_rgb_to_hex(new)
            var = self._choices[bid]
            ttk.Label(inner, text=bid, width=40, anchor="w").grid(
                row=row_i, column=0, sticky="w", padx=6, pady=2)
            cur_fr = ttk.Frame(inner)
            cur_fr.grid(row=row_i, column=1, sticky="w", padx=4, pady=2)
            try:
                tk.Label(cur_fr, bg=cur_hex, width=3, height=1,
                         relief="solid", bd=1).pack(side="left")
            except Exception:
                pass
            ttk.Label(cur_fr, text=f" {cur_hex}").pack(side="left")
            new_fr = ttk.Frame(inner)
            new_fr.grid(row=row_i, column=2, sticky="w", padx=4, pady=2)
            try:
                tk.Label(new_fr, bg=new_hex, width=3, height=1,
                         relief="solid", bd=1).pack(side="left")
            except Exception:
                pass
            ttk.Label(new_fr, text=f" {new_hex}").pack(side="left")
            choice_fr = ttk.Frame(inner)
            choice_fr.grid(row=row_i, column=3, sticky="w", padx=4, pady=2)
            ttk.Radiobutton(choice_fr, text="Current",
                            variable=var, value="current").pack(side="left")
            ttk.Radiobutton(choice_fr, text="New",
                            variable=var, value="new").pack(side="left", padx=(4, 0))

        inner.grid_columnconfigure(0, weight=1)

        btns = ttk.Frame(self)
        btns.pack(fill="x", padx=14, pady=(4, 10))
        ttk.Button(btns, text="Cancel", command=self._cancel).pack(side="right")
        ttk.Button(btns, text="Apply", command=self._apply).pack(side="right", padx=(0, 8))

    def _on_mw(self, evt) -> None:
        self._canvas.yview_scroll(-1 if getattr(evt, "delta", 0) > 0 else 1, "units")

    def _set_all(self, choice: str) -> None:
        for var in self._choices.values():
            var.set(choice)

    def _apply(self) -> None:
        self.resolutions = {
            bid: (cur if self._choices[bid].get() == "current" else new)
            for bid, cur, new in self._conflicts
        }
        self.destroy()

    def _cancel(self) -> None:
        self.destroy()


class _TexturePackPreviewDialog(tk.Toplevel):
    """Preview block colors extracted from a texture pack and select which to import.

    Shows a filterable, checkable grid of block_id + color swatch + hex.
    ``result_palette`` is set to the selected subset on confirm, or ``None`` if cancelled.
    """

    def __init__(self, parent: tk.Misc,
                 extracted: Dict[str, _PalRGB], warnings: List[str]):
        super().__init__(parent)
        self.title(f"Texture Pack Colors — {len(extracted):,} blocks found")
        self.geometry("820x580")
        self.minsize(600, 420)
        self.transient(parent)
        self.grab_set()
        self.resizable(True, True)

        self.result_palette: Optional[Dict[str, _PalRGB]] = None
        self._all_keys = sorted(extracted.keys())
        self._extracted = extracted
        self._vars: Dict[str, tk.BooleanVar] = {
            k: tk.BooleanVar(value=True) for k in self._all_keys
        }
        self._filter_var = tk.StringVar()

        top = ttk.Frame(self)
        top.pack(fill="x", padx=12, pady=(10, 4))
        ttk.Label(top, text="Filter:").pack(side="left")
        ent = ttk.Entry(top, textvariable=self._filter_var, width=28)
        ent.pack(side="left", padx=(6, 12))
        ent.bind("<KeyRelease>", lambda _e: self._rebuild())
        ttk.Button(top, text="Select all",
                   command=self._select_all).pack(side="left")
        ttk.Button(top, text="Deselect all",
                   command=self._deselect_all).pack(side="left", padx=(4, 0))
        if warnings:
            ttk.Label(top,
                      text=f"  \u26a0 {len(warnings)} texture(s) skipped",
                      foreground="#c07000").pack(side="left", padx=(12, 0))

        canvas = tk.Canvas(self, highlightthickness=0)
        vsb = ttk.Scrollbar(self, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")
        canvas.pack(fill="both", expand=True, padx=12, pady=4)
        self._inner = ttk.Frame(canvas)
        _win = canvas.create_window(0, 0, window=self._inner, anchor="nw")
        self._inner.bind("<Configure>",
                         lambda _e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.bind("<Configure>",
                    lambda e: canvas.itemconfigure(_win, width=e.width))
        canvas.bind("<Enter>",
                    lambda _e: canvas.bind_all("<MouseWheel>", self._on_mw))
        canvas.bind("<Leave>",
                    lambda _e: canvas.unbind_all("<MouseWheel>"))
        self._canvas = canvas

        ent.focus_set()
        self._rebuild()

        btns = ttk.Frame(self)
        btns.pack(fill="x", padx=12, pady=(4, 10))
        ttk.Button(btns, text="Cancel", command=self._cancel).pack(side="right")
        ttk.Button(btns, text="Import selected",
                   command=self._apply).pack(side="right", padx=(0, 8))

    def _on_mw(self, evt) -> None:
        self._canvas.yview_scroll(-1 if getattr(evt, "delta", 0) > 0 else 1, "units")

    def _rebuild(self) -> None:
        for w in self._inner.winfo_children():
            w.destroy()
        flt = self._filter_var.get().strip().lower()
        keys = [k for k in self._all_keys if not flt or flt in k.lower()]
        cols = 3
        for i, k in enumerate(keys):
            rgb = self._extracted[k]
            hex_ = _pal_rgb_to_hex(rgb)
            fr = ttk.Frame(self._inner)
            fr.grid(row=i // cols, column=i % cols, sticky="w", padx=6, pady=2)
            ttk.Checkbutton(fr, variable=self._vars[k]).pack(side="left")
            try:
                tk.Label(fr, bg=hex_, width=2, height=1,
                         relief="solid", bd=1).pack(side="left", padx=(2, 4))
            except Exception:
                pass
            ttk.Label(fr, text=f"{k}  {hex_}", anchor="w").pack(side="left")
        for c in range(cols):
            self._inner.grid_columnconfigure(c, weight=1)

    def _select_all(self) -> None:
        flt = self._filter_var.get().strip().lower()
        for k in self._all_keys:
            if not flt or flt in k.lower():
                self._vars[k].set(True)

    def _deselect_all(self) -> None:
        flt = self._filter_var.get().strip().lower()
        for k in self._all_keys:
            if not flt or flt in k.lower():
                self._vars[k].set(False)

    def _apply(self) -> None:
        self.result_palette = {
            k: self._extracted[k] for k in self._all_keys if self._vars[k].get()
        }
        self.destroy()

    def _cancel(self) -> None:
        self.destroy()


def _safe_rmtree(path: str, log=None, max_tries: int = 8) -> None:
    """Best-effort recursive delete with retries (Windows AV/indexer/file-lock friendly)."""
    if not path or not os.path.exists(path):
        return
    for i in range(max_tries):
        try:
            shutil.rmtree(path, ignore_errors=False)
            return
        except Exception as e:
            if log:
                log(f"Cleanup retry {i+1}/{max_tries} failed for {path}: {e}")
            time.sleep(0.15 * (i + 1))
    # last resort
    try:
        shutil.rmtree(path, ignore_errors=True)
    except Exception:
        pass

def _atomic_save_png(img, out_path: str, log=None, max_tries: int = 8) -> None:
    """Save PNG atomically with retries (avoids partial/locked files)."""
    tmp_path = out_path + ".tmp"
    last_err = None
    for i in range(max_tries):
        try:
            img.save(tmp_path, format="PNG")
            os.replace(tmp_path, out_path)
            return
        except Exception as e:
            last_err = e
            if log:
                log(f"PNG save retry {i+1}/{max_tries} failed for {out_path}: {e}")
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass
    raise RuntimeError(f"Failed to save PNG after {max_tries} attempts: {out_path}; last error: {last_err}")


# =============================================================================
# App identity (versioning)
# =============================================================================

APP_NAME = "World Map Timeline Tool for Minecraft"

def _build_state_file_path() -> str:
    """Prefer app directory; fall back to LOCALAPPDATA when not writable."""
    base_dir = os.path.dirname(sys.executable) if getattr(sys, "frozen", False) else os.path.dirname(os.path.abspath(__file__))
    primary = os.path.join(base_dir, ".wmtt4mc_build_state.json")
    try:
        os.makedirs(base_dir, exist_ok=True)
        with open(primary + ".tmp", "w", encoding="utf-8") as f:
            f.write("{}")
        os.remove(primary + ".tmp")
        return primary
    except Exception:
        local = os.getenv("LOCALAPPDATA") or os.path.expanduser("~")
        fallback_dir = os.path.join(local, "WMTT4MC")
        try:
            os.makedirs(fallback_dir, exist_ok=True)
        except Exception:
            pass
        return os.path.join(fallback_dir, ".wmtt4mc_build_state.json")


def _source_fingerprint() -> str:
    """Stable fingerprint of code relevant to the current run."""
    h = hashlib.sha256()
    if getattr(sys, "frozen", False):
        exe_path = sys.executable
        try:
            st = os.stat(exe_path)
            payload = f"{os.path.basename(exe_path)}|{st.st_size}|{st.st_mtime_ns}".encode("utf-8")
            h.update(payload)
        except Exception:
            h.update(exe_path.encode("utf-8", errors="ignore"))
        return h.hexdigest()

    root = os.path.dirname(os.path.abspath(__file__))
    candidates: List[str] = []
    try:
        for name in os.listdir(root):
            if name.endswith(".py") or name.endswith(".spec"):
                candidates.append(os.path.join(root, name))
    except Exception:
        candidates.append(os.path.abspath(__file__))

    for path in sorted(candidates):
        try:
            rel = os.path.relpath(path, root).replace("\\", "/")
            h.update(rel.encode("utf-8"))
            with open(path, "rb") as f:
                while True:
                    chunk = f.read(65536)
                    if not chunk:
                        break
                    h.update(chunk)
        except Exception:
            h.update(path.encode("utf-8", errors="ignore"))
    return h.hexdigest()


def _load_build_state(path: str) -> Dict[str, Any]:
    try:
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
    except Exception:
        pass
    return {}


def _save_build_state(path: str, data: Dict[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, path)
    except Exception:
        pass


def _get_next_build_number() -> str:
    """Build format: YYYY.MM.DD.NN, incrementing once per code change per day."""
    today = datetime.now().strftime("%Y.%m.%d")
    source_hash = _source_fingerprint()
    state_path = _build_state_file_path()
    state = _load_build_state(state_path)

    prev_date = str(state.get("date", ""))
    prev_hash = str(state.get("source_hash", ""))
    try:
        prev_build = int(state.get("build", 0))
    except Exception:
        prev_build = 0

    force_bump = str(os.getenv("WMTT4MC_FORCE_BUILD_BUMP", "")).strip().lower() in {"1", "true", "yes", "y"}

    if prev_date != today:
        build_num = 1
    elif force_bump:
        build_num = max(1, prev_build + 1)
    elif prev_hash != source_hash:
        build_num = max(1, prev_build + 1)
    else:
        build_num = max(1, prev_build)

    _save_build_state(state_path, {
        "date": today,
        "build": build_num,
        "source_hash": source_hash,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    })
    return f"{today}.{build_num:02d}"

APP_VERSION = "1.7.0"
# Build number increments per code change (and can be force-bumped with WMTT4MC_FORCE_BUILD_BUMP=1).
APP_BUILD = _get_next_build_number()
APP_ABBR = "WMTT4MC"
DISCLAIMER_TEXT = "NOT AN OFFICIAL MINECRAFT PRODUCT. Not affiliated with or endorsed by Mojang or Microsoft."


# =============================================================================
# Palette / block classification
# =============================================================================

def build_palette_lookup(palette_dict):
    """Builds a (str->int) mapping and a numpy color table for fast lookup."""
    keys = list(palette_dict.keys())
    color_table = np.zeros((len(keys), 3), dtype=np.uint8)
    key_to_idx = {}
    for i, k in enumerate(keys):
        color_table[i] = palette_dict[k]
        key_to_idx[k] = i
    return key_to_idx, color_table

PALETTE = {
    'minecraft:acacia_door_bottom': (199, 178, 85),
    'minecraft:acacia_door_top': (199, 178, 85),
    'minecraft:acacia_leaves': (149, 149, 149),
    'minecraft:acacia_log': (103, 97, 87),
    'minecraft:acacia_log_top': (151, 89, 55),
    'minecraft:acacia_planks': (168, 90, 50),
    'minecraft:acacia_sapling': (119, 118, 24),
    'minecraft:acacia_shelf': (156, 86, 54),
    'minecraft:acacia_trapdoor': (157, 87, 51),
    'minecraft:activator_rail': (115, 87, 74),
    'minecraft:activator_rail_on': (193, 65, 35),
    'minecraft:allium': (159, 137, 184),
    'minecraft:amethyst_block': (134, 98, 191),
    'minecraft:amethyst_cluster': (164, 127, 207),
    'minecraft:ancient_debris_side': (96, 64, 56),
    'minecraft:ancient_debris_top': (95, 66, 58),
    'minecraft:andesite': (136, 136, 137),
    'minecraft:anvil': (69, 69, 69),
    'minecraft:anvil_top': (73, 73, 73),
    'minecraft:attached_melon_stem': (142, 142, 142),
    'minecraft:attached_pumpkin_stem': (139, 139, 139),
    'minecraft:azalea_leaves': (90, 115, 44),
    'minecraft:azalea_plant': (92, 110, 43),
    'minecraft:azalea_side': (94, 118, 45),
    'minecraft:azalea_top': (102, 125, 48),
    'minecraft:azure_bluet': (169, 205, 127),
    'minecraft:bamboo_block': (127, 144, 58),
    'minecraft:bamboo_block_top': (139, 142, 62),
    'minecraft:bamboo_door_bottom': (199, 178, 85),
    'minecraft:bamboo_door_top': (199, 178, 85),
    'minecraft:bamboo_fence': (207, 186, 89),
    'minecraft:bamboo_fence_gate': (206, 186, 87),
    'minecraft:bamboo_fence_gate_particle': (207, 186, 85),
    'minecraft:bamboo_fence_particle': (207, 186, 85),
    'minecraft:bamboo_large_leaves': (73, 118, 26),
    'minecraft:bamboo_mosaic': (190, 170, 78),
    'minecraft:bamboo_planks': (193, 173, 80),
    'minecraft:bamboo_shelf': (182, 164, 76),
    'minecraft:bamboo_singleleaf': (100, 139, 35),
    'minecraft:bamboo_small_leaves': (71, 113, 26),
    'minecraft:bamboo_stage0': (92, 89, 36),
    'minecraft:bamboo_stalk': (94, 144, 20),
    'minecraft:bamboo_trapdoor': (199, 179, 85),
    'minecraft:barrel_bottom': (116, 85, 49),
    'minecraft:barrel_side': (108, 81, 50),
    'minecraft:barrel_top': (135, 101, 58),
    'minecraft:barrel_top_open': (93, 68, 34),
    'minecraft:basalt_side': (73, 73, 78),
    'minecraft:basalt_top': (81, 81, 86),
    'minecraft:beacon': (118, 221, 215),
    'minecraft:bedrock': (85, 85, 85),
    'minecraft:bee_nest_bottom': (161, 127, 88),
    'minecraft:bee_nest_front': (183, 142, 76),
    'minecraft:bee_nest_front_honey': (195, 152, 76),
    'minecraft:bee_nest_side': (196, 151, 77),
    'minecraft:bee_nest_top': (202, 160, 75),
    'minecraft:beehive_end': (181, 146, 90),
    'minecraft:beehive_front': (159, 128, 78),
    'minecraft:beehive_front_honey': (167, 132, 74),
    'minecraft:beehive_side': (157, 126, 76),
    'minecraft:beetroots_stage0': (66, 138, 41),
    'minecraft:beetroots_stage1': (67, 139, 41),
    'minecraft:beetroots_stage2': (69, 131, 39),
    'minecraft:beetroots_stage3': (93, 92, 30),
    'minecraft:bell_bottom': (189, 148, 42),
    'minecraft:bell_side': (253, 229, 97),
    'minecraft:bell_top': (253, 235, 111),
    'minecraft:big_dripleaf_side': (75, 98, 44),
    'minecraft:big_dripleaf_stem': (91, 115, 46),
    'minecraft:big_dripleaf_tip': (98, 125, 48),
    'minecraft:big_dripleaf_top': (112, 142, 52),
    'minecraft:birch_door_bottom': (199, 178, 85),
    'minecraft:birch_door_top': (199, 178, 85),
    'minecraft:birch_leaves': (131, 129, 130),
    'minecraft:birch_log': (217, 215, 210),
    'minecraft:birch_log_top': (193, 179, 135),
    'minecraft:birch_planks': (192, 175, 121),
    'minecraft:birch_sapling': (128, 161, 80),
    'minecraft:birch_shelf': (175, 156, 106),
    'minecraft:birch_trapdoor': (207, 194, 157),
    'minecraft:black_candle': (38, 37, 58),
    'minecraft:black_candle_lit': (74, 49, 52),
    'minecraft:black_concrete': (8, 10, 15),
    'minecraft:black_concrete_powder': (25, 27, 32),
    'minecraft:black_glazed_terracotta': (68, 30, 32),
    'minecraft:black_shulker_box': (25, 25, 30),
    'minecraft:black_stained_glass': (25, 25, 25),
    'minecraft:black_stained_glass_pane_top': (24, 24, 24),
    'minecraft:black_terracotta': (37, 23, 16),
    'minecraft:black_wool': (21, 21, 26),
    'minecraft:blackstone': (42, 36, 41),
    'minecraft:blackstone_top': (42, 36, 42),
    'minecraft:blast_furnace_front': (108, 108, 107),
    'minecraft:blast_furnace_front_on': (116, 111, 105),
    'minecraft:blast_furnace_side': (108, 107, 108),
    'minecraft:blast_furnace_top': (81, 80, 81),
    'minecraft:blue_candle': (57, 76, 161),
    'minecraft:blue_candle_lit': (65, 85, 179),
    'minecraft:blue_concrete': (45, 47, 143),
    'minecraft:blue_concrete_powder': (70, 73, 167),
    'minecraft:blue_glazed_terracotta': (47, 65, 139),
    'minecraft:blue_ice': (116, 168, 253),
    'minecraft:blue_orchid': (47, 162, 168),
    'minecraft:blue_shulker_box': (44, 46, 140),
    'minecraft:blue_stained_glass': (51, 76, 178),
    'minecraft:blue_stained_glass_pane_top': (49, 74, 171),
    'minecraft:blue_terracotta': (74, 60, 91),
    'minecraft:blue_wool': (53, 57, 157),
    'minecraft:bone_block_side': (229, 226, 208),
    'minecraft:bone_block_top': (210, 206, 179),
    'minecraft:bookshelf': (117, 95, 60),
    'minecraft:brain_coral': (198, 85, 152),
    'minecraft:brain_coral_block': (207, 91, 159),
    'minecraft:brain_coral_fan': (203, 84, 154),
    'minecraft:brewing_stand': (122, 101, 81),
    'minecraft:brewing_stand_base': (117, 106, 106),
    'minecraft:bricks': (151, 98, 83),
    'minecraft:brown_candle': (112, 70, 41),
    'minecraft:brown_candle_lit': (148, 96, 52),
    'minecraft:brown_concrete': (96, 60, 32),
    'minecraft:brown_concrete_powder': (126, 85, 54),
    'minecraft:brown_glazed_terracotta': (120, 106, 86),
    'minecraft:brown_mushroom': (154, 117, 92),
    'minecraft:brown_mushroom_block': (149, 112, 81),
    'minecraft:brown_shulker_box': (106, 66, 36),
    'minecraft:brown_stained_glass': (102, 76, 51),
    'minecraft:brown_stained_glass_pane_top': (98, 74, 49),
    'minecraft:brown_terracotta': (77, 51, 36),
    'minecraft:brown_wool': (114, 72, 41),
    'minecraft:bubble_coral': (161, 24, 160),
    'minecraft:bubble_coral_block': (165, 26, 162),
    'minecraft:bubble_coral_fan': (160, 33, 159),
    'minecraft:budding_amethyst': (132, 96, 187),
    'minecraft:bush': (120, 121, 120),
    'minecraft:cactus_bottom': (143, 170, 86),
    'minecraft:cactus_flower': (210, 121, 135),
    'minecraft:cactus_side': (89, 130, 45),
    'minecraft:cactus_top': (86, 127, 43),
    'minecraft:cake_bottom': (134, 62, 33),
    'minecraft:cake_inner': (134, 85, 61),
    'minecraft:cake_side': (203, 152, 122),
    'minecraft:cake_top': (248, 223, 214),
    'minecraft:calcite': (223, 224, 221),
    'minecraft:calibrated_sculk_sensor_amethyst': (199, 160, 225),
    'minecraft:calibrated_sculk_sensor_input_side': (60, 70, 102),
    'minecraft:calibrated_sculk_sensor_top': (28, 79, 101),
    'minecraft:campfire_fire': (219, 158, 58),
    'minecraft:campfire_log': (79, 75, 68),
    'minecraft:campfire_log_lit': (111, 88, 54),
    'minecraft:candle': (232, 201, 153),
    'minecraft:candle_lit': (235, 210, 174),
    'minecraft:carrots_stage0': (45, 110, 40),
    'minecraft:carrots_stage1': (53, 120, 40),
    'minecraft:carrots_stage2': (57, 114, 38),
    'minecraft:carrots_stage3': (82, 124, 38),
    'minecraft:cartography_table_side1': (70, 50, 34),
    'minecraft:cartography_table_side2': (82, 62, 42),
    'minecraft:cartography_table_side3': (68, 44, 20),
    'minecraft:cartography_table_top': (103, 87, 67),
    'minecraft:carved_pumpkin': (150, 84, 17),
    'minecraft:cauldron_bottom': (40, 40, 45),
    'minecraft:cauldron_inner': (49, 49, 53),
    'minecraft:cauldron_side': (74, 73, 75),
    'minecraft:cauldron_top': (74, 73, 74),
    'minecraft:cave_vines': (90, 109, 41),
    'minecraft:cave_vines_lit': (105, 113, 42),
    'minecraft:cave_vines_plant': (88, 102, 38),
    'minecraft:cave_vines_plant_lit': (105, 107, 40),
    'minecraft:chain_command_block_back': (130, 157, 145),
    'minecraft:chain_command_block_conditional': (130, 162, 147),
    'minecraft:chain_command_block_front': (132, 165, 151),
    'minecraft:chain_command_block_side': (131, 161, 147),
    'minecraft:cherry_door_bottom': (199, 178, 85),
    'minecraft:cherry_door_top': (199, 178, 85),
    'minecraft:cherry_leaves': (229, 173, 194),
    'minecraft:cherry_log': (55, 33, 44),
    'minecraft:cherry_log_top': (185, 141, 137),
    'minecraft:cherry_planks': (227, 179, 173),
    'minecraft:cherry_sapling': (164, 118, 143),
    'minecraft:cherry_shelf': (203, 130, 129),
    'minecraft:cherry_trapdoor': (226, 179, 172),
    'minecraft:chipped_anvil_top': (73, 73, 73),
    'minecraft:chiseled_bookshelf_empty': (90, 71, 42),
    'minecraft:chiseled_bookshelf_occupied': (121, 94, 70),
    'minecraft:chiseled_bookshelf_side': (175, 142, 86),
    'minecraft:chiseled_bookshelf_top': (178, 145, 89),
    'minecraft:chiseled_copper': (184, 101, 74),
    'minecraft:chiseled_deepslate': (54, 54, 55),
    'minecraft:chiseled_nether_bricks': (46, 23, 27),
    'minecraft:chiseled_polished_blackstone': (54, 49, 57),
    'minecraft:chiseled_quartz_block': (232, 227, 218),
    'minecraft:chiseled_quartz_block_top': (232, 227, 217),
    'minecraft:chiseled_red_sandstone': (183, 97, 28),
    'minecraft:chiseled_resin_bricks': (201, 84, 25),
    'minecraft:chiseled_sandstone': (216, 203, 155),
    'minecraft:chiseled_stone_bricks': (120, 119, 120),
    'minecraft:chiseled_tuff': (89, 94, 87),
    'minecraft:chiseled_tuff_bricks': (99, 103, 96),
    'minecraft:chiseled_tuff_bricks_top': (111, 114, 107),
    'minecraft:chiseled_tuff_top': (94, 99, 91),
    'minecraft:chorus_flower': (151, 121, 152),
    'minecraft:chorus_flower_dead': (97, 61, 95),
    'minecraft:chorus_plant': (94, 57, 94),
    'minecraft:clay': (161, 166, 179),
    'minecraft:closed_eyeblossom': (108, 98, 101),
    'minecraft:coal_block': (16, 16, 16),
    'minecraft:coal_ore': (106, 106, 105),
    'minecraft:coarse_dirt': (119, 86, 59),
    'minecraft:cobbled_deepslate': (77, 77, 81),
    'minecraft:cobblestone': (128, 127, 128),
    'minecraft:cobweb': (229, 233, 234),
    'minecraft:cocoa_stage0': (133, 135, 62),
    'minecraft:cocoa_stage1': (146, 111, 56),
    'minecraft:cocoa_stage2': (154, 92, 41),
    'minecraft:command_block_back': (174, 131, 107),
    'minecraft:command_block_conditional': (179, 133, 106),
    'minecraft:command_block_front': (181, 136, 108),
    'minecraft:command_block_side': (177, 133, 108),
    'minecraft:comparator': (166, 162, 160),
    'minecraft:comparator_on': (176, 160, 158),
    'minecraft:composter_bottom': (117, 72, 32),
    'minecraft:composter_compost': (89, 61, 24),
    'minecraft:composter_ready': (117, 93, 56),
    'minecraft:composter_side': (112, 70, 32),
    'minecraft:composter_top': (153, 99, 52),
    'minecraft:conduit': (160, 140, 113),
    'minecraft:copper_bars': (156, 81, 55),
    'minecraft:copper_block': (192, 108, 80),
    'minecraft:copper_bulb': (156, 87, 57),
    'minecraft:copper_bulb_lit': (216, 151, 107),
    'minecraft:copper_bulb_lit_powered': (216, 150, 106),
    'minecraft:copper_bulb_powered': (157, 86, 57),
    'minecraft:copper_chain': (152, 79, 55),
    'minecraft:copper_door_bottom': (199, 178, 85),
    'minecraft:copper_door_top': (199, 178, 85),
    'minecraft:copper_grate': (192, 108, 79),
    'minecraft:copper_lantern': (158, 108, 76),
    'minecraft:copper_ore': (125, 126, 120),
    'minecraft:copper_torch': (117, 110, 73),
    'minecraft:copper_trapdoor': (191, 106, 80),
    'minecraft:cornflower': (80, 121, 147),
    'minecraft:cracked_deepslate_bricks': (65, 65, 65),
    'minecraft:cracked_deepslate_tiles': (53, 53, 53),
    'minecraft:cracked_nether_bricks': (40, 20, 24),
    'minecraft:cracked_polished_blackstone_bricks': (44, 38, 44),
    'minecraft:cracked_stone_bricks': (118, 118, 118),
    'minecraft:crafter_bottom': (79, 79, 79),
    'minecraft:crafter_east': (127, 114, 93),
    'minecraft:crafter_east_crafting': (129, 114, 93),
    'minecraft:crafter_east_triggered': (129, 114, 93),
    'minecraft:crafter_north': (114, 109, 101),
    'minecraft:crafter_north_crafting': (104, 96, 82),
    'minecraft:crafter_south': (123, 112, 97),
    'minecraft:crafter_south_triggered': (126, 113, 97),
    'minecraft:crafter_top': (112, 99, 100),
    'minecraft:crafter_top_crafting': (140, 100, 100),
    'minecraft:crafter_top_triggered': (115, 99, 100),
    'minecraft:crafter_west': (129, 115, 95),
    'minecraft:crafter_west_crafting': (130, 116, 95),
    'minecraft:crafter_west_triggered': (130, 115, 95),
    'minecraft:crafting_table_front': (129, 106, 70),
    'minecraft:crafting_table_side': (129, 103, 63),
    'minecraft:crafting_table_top': (120, 73, 42),
    'minecraft:creaking_heart': (82, 68, 63),
    'minecraft:creaking_heart_awake': (150, 84, 49),
    'minecraft:creaking_heart_dormant': (102, 66, 49),
    'minecraft:creaking_heart_top': (73, 60, 54),
    'minecraft:creaking_heart_top_awake': (153, 84, 45),
    'minecraft:creaking_heart_top_dormant': (102, 65, 45),
    'minecraft:crimson_door_bottom': (199, 178, 85),
    'minecraft:crimson_door_top': (199, 178, 85),
    'minecraft:crimson_fungus': (141, 44, 30),
    'minecraft:crimson_nylium': (131, 31, 31),
    'minecraft:crimson_nylium_side': (107, 27, 27),
    'minecraft:crimson_planks': (101, 49, 71),
    'minecraft:crimson_roots': (126, 8, 42),
    'minecraft:crimson_roots_pot': (127, 8, 42),
    'minecraft:crimson_shelf': (129, 52, 83),
    'minecraft:crimson_stem': (93, 26, 30),
    'minecraft:crimson_stem_top': (113, 50, 70),
    'minecraft:crimson_trapdoor': (104, 51, 72),
    'minecraft:crying_obsidian': (33, 10, 60),
    'minecraft:cut_copper': (191, 107, 81),
    'minecraft:cut_red_sandstone': (189, 102, 32),
    'minecraft:cut_sandstone': (218, 206, 160),
    'minecraft:cyan_candle': (17, 124, 124),
    'minecraft:cyan_candle_lit': (43, 147, 135),
    'minecraft:cyan_concrete': (21, 119, 136),
    'minecraft:cyan_concrete_powder': (37, 148, 157),
    'minecraft:cyan_glazed_terracotta': (52, 119, 125),
    'minecraft:cyan_shulker_box': (20, 121, 135),
    'minecraft:cyan_stained_glass': (76, 127, 153),
    'minecraft:cyan_stained_glass_pane_top': (74, 123, 147),
    'minecraft:cyan_terracotta': (87, 91, 91),
    'minecraft:cyan_wool': (21, 138, 145),
    'minecraft:damaged_anvil_top': (72, 72, 72),
    'minecraft:dandelion': (148, 172, 43),
    'minecraft:dark_oak_door_bottom': (199, 178, 85),
    'minecraft:dark_oak_door_top': (199, 178, 85),
    'minecraft:dark_oak_leaves': (151, 151, 151),
    'minecraft:dark_oak_log': (60, 47, 26),
    'minecraft:dark_oak_log_top': (68, 45, 22),
    'minecraft:dark_oak_planks': (67, 43, 20),
    'minecraft:dark_oak_sapling': (61, 91, 31),
    'minecraft:dark_oak_shelf': (66, 51, 33),
    'minecraft:dark_oak_trapdoor': (75, 50, 23),
    'minecraft:dark_prismarine': (52, 92, 76),
    'minecraft:daylight_detector_inverted_top': (106, 109, 113),
    'minecraft:daylight_detector_side': (67, 55, 36),
    'minecraft:daylight_detector_top': (131, 116, 95),
    'minecraft:dead_brain_coral': (134, 125, 121),
    'minecraft:dead_brain_coral_block': (124, 118, 114),
    'minecraft:dead_brain_coral_fan': (133, 125, 121),
    'minecraft:dead_bubble_coral': (133, 125, 121),
    'minecraft:dead_bubble_coral_block': (132, 124, 119),
    'minecraft:dead_bubble_coral_fan': (141, 135, 130),
    'minecraft:dead_bush': (107, 79, 41),
    'minecraft:dead_fire_coral': (137, 128, 124),
    'minecraft:dead_fire_coral_block': (132, 124, 120),
    'minecraft:dead_fire_coral_fan': (125, 118, 115),
    'minecraft:dead_horn_coral': (143, 135, 130),
    'minecraft:dead_horn_coral_block': (134, 126, 122),
    'minecraft:dead_horn_coral_fan': (134, 126, 121),
    'minecraft:dead_tube_coral': (118, 111, 108),
    'minecraft:dead_tube_coral_block': (130, 123, 120),
    'minecraft:dead_tube_coral_fan': (128, 122, 118),
    'minecraft:debug': (133, 148, 153),
    'minecraft:debug2': (124, 120, 118),
    'minecraft:deepslate': (80, 80, 83),
    'minecraft:deepslate_bricks': (71, 71, 71),
    'minecraft:deepslate_coal_ore': (74, 74, 76),
    'minecraft:deepslate_copper_ore': (92, 93, 89),
    'minecraft:deepslate_diamond_ore': (83, 106, 107),
    'minecraft:deepslate_emerald_ore': (78, 104, 88),
    'minecraft:deepslate_gold_ore': (115, 103, 78),
    'minecraft:deepslate_iron_ore': (107, 100, 95),
    'minecraft:deepslate_lapis_ore': (80, 91, 115),
    'minecraft:deepslate_redstone_ore': (105, 73, 75),
    'minecraft:deepslate_tiles': (55, 55, 55),
    'minecraft:deepslate_top': (87, 87, 89),
    'minecraft:destroy_stage_0': (253, 253, 253),
    'minecraft:destroy_stage_1': (249, 249, 249),
    'minecraft:destroy_stage_2': (244, 244, 244),
    'minecraft:destroy_stage_3': (238, 238, 238),
    'minecraft:destroy_stage_4': (231, 231, 231),
    'minecraft:destroy_stage_5': (222, 222, 222),
    'minecraft:destroy_stage_6': (213, 213, 213),
    'minecraft:destroy_stage_7': (195, 195, 195),
    'minecraft:destroy_stage_8': (178, 178, 178),
    'minecraft:destroy_stage_9': (168, 168, 168),
    'minecraft:detector_rail': (123, 105, 90),
    'minecraft:detector_rail_on': (137, 103, 89),
    'minecraft:diamond_block': (98, 237, 228),
    'minecraft:diamond_ore': (121, 141, 141),
    'minecraft:diorite': (189, 188, 189),
    'minecraft:dirt': (134, 96, 67),
    'minecraft:dirt_path_side': (137, 101, 67),
    'minecraft:dirt_path_top': (148, 122, 65),
    'minecraft:dispenser_front': (122, 122, 122),
    'minecraft:dispenser_front_vertical': (99, 98, 98),
    'minecraft:dragon_egg': (13, 9, 16),
    'minecraft:dried_ghast_hydration_0_bottom': (146, 136, 136),
    'minecraft:dried_ghast_hydration_0_east': (159, 145, 145),
    'minecraft:dried_ghast_hydration_0_north': (158, 144, 144),
    'minecraft:dried_ghast_hydration_0_south': (159, 146, 146),
    'minecraft:dried_ghast_hydration_0_tentacles': (119, 117, 117),
    'minecraft:dried_ghast_hydration_0_top': (159, 145, 145),
    'minecraft:dried_ghast_hydration_0_west': (163, 148, 148),
    'minecraft:dried_ghast_hydration_1_bottom': (167, 159, 159),
    'minecraft:dried_ghast_hydration_1_east': (179, 168, 168),
    'minecraft:dried_ghast_hydration_1_north': (179, 168, 168),
    'minecraft:dried_ghast_hydration_1_south': (177, 167, 167),
    'minecraft:dried_ghast_hydration_1_tentacles': (145, 142, 142),
    'minecraft:dried_ghast_hydration_1_top': (179, 168, 168),
    'minecraft:dried_ghast_hydration_1_west': (182, 171, 171),
    'minecraft:dried_ghast_hydration_2_bottom': (189, 183, 183),
    'minecraft:dried_ghast_hydration_2_east': (199, 192, 192),
    'minecraft:dried_ghast_hydration_2_north': (199, 191, 191),
    'minecraft:dried_ghast_hydration_2_south': (197, 190, 190),
    'minecraft:dried_ghast_hydration_2_tentacles': (171, 169, 169),
    'minecraft:dried_ghast_hydration_2_top': (199, 192, 192),
    'minecraft:dried_ghast_hydration_2_west': (202, 195, 195),
    'minecraft:dried_ghast_hydration_3_bottom': (235, 232, 232),
    'minecraft:dried_ghast_hydration_3_east': (241, 241, 241),
    'minecraft:dried_ghast_hydration_3_north': (239, 239, 239),
    'minecraft:dried_ghast_hydration_3_south': (240, 239, 239),
    'minecraft:dried_ghast_hydration_3_tentacles': (224, 221, 221),
    'minecraft:dried_ghast_hydration_3_top': (241, 240, 240),
    'minecraft:dried_ghast_hydration_3_west': (243, 243, 243),
    'minecraft:dried_kelp_bottom': (50, 59, 39),
    'minecraft:dried_kelp_side': (38, 49, 30),
    'minecraft:dried_kelp_top': (50, 59, 39),
    'minecraft:dripstone_block': (134, 108, 93),
    'minecraft:dropper_front': (122, 122, 122),
    'minecraft:dropper_front_vertical': (98, 97, 97),
    'minecraft:emerald_block': (42, 203, 88),
    'minecraft:emerald_ore': (108, 136, 116),
    'minecraft:enchanting_table_bottom': (15, 11, 25),
    'minecraft:enchanting_table_side': (50, 46, 57),
    'minecraft:enchanting_table_top': (129, 75, 85),
    'minecraft:end_portal_frame_eye': (36, 70, 62),
    'minecraft:end_portal_frame_side': (151, 163, 123),
    'minecraft:end_portal_frame_top': (91, 121, 97),
    'minecraft:end_rod': (208, 198, 187),
    'minecraft:end_stone': (220, 223, 158),
    'minecraft:end_stone_bricks': (218, 224, 162),
    'minecraft:exposed_chiseled_copper': (155, 119, 101),
    'minecraft:exposed_copper': (161, 126, 104),
    'minecraft:exposed_copper_bars': (134, 108, 89),
    'minecraft:exposed_copper_bulb': (135, 108, 90),
    'minecraft:exposed_copper_bulb_lit': (194, 145, 100),
    'minecraft:exposed_copper_bulb_lit_powered': (195, 144, 99),
    'minecraft:exposed_copper_bulb_powered': (136, 106, 89),
    'minecraft:exposed_copper_chain': (126, 102, 84),
    'minecraft:exposed_copper_door_bottom': (199, 178, 85),
    'minecraft:exposed_copper_door_top': (199, 178, 85),
    'minecraft:exposed_copper_grate': (162, 126, 105),
    'minecraft:exposed_copper_lantern': (148, 131, 105),
    'minecraft:exposed_copper_trapdoor': (161, 125, 105),
    'minecraft:exposed_cut_copper': (155, 122, 101),
    'minecraft:exposed_lightning_rod': (159, 120, 104),
    'minecraft:farmland': (143, 103, 71),
    'minecraft:farmland_moist': (82, 44, 15),
    'minecraft:fern': (124, 125, 124),
    'minecraft:fire_0': (212, 140, 54),
    'minecraft:fire_1': (211, 138, 50),
    'minecraft:fire_coral': (167, 38, 47),
    'minecraft:fire_coral_block': (164, 35, 47),
    'minecraft:fire_coral_fan': (159, 35, 46),
    'minecraft:firefly_bush': (88, 83, 43),
    'minecraft:firefly_bush_emissive': (193, 192, 159),
    'minecraft:fletching_table_front': (173, 155, 111),
    'minecraft:fletching_table_side': (192, 167, 130),
    'minecraft:fletching_table_top': (197, 180, 133),
    'minecraft:flower_pot': (124, 69, 53),
    'minecraft:flowering_azalea_leaves': (100, 111, 61),
    'minecraft:flowering_azalea_side': (111, 113, 73),
    'minecraft:flowering_azalea_top': (112, 122, 64),
    'minecraft:frogspawn': (110, 95, 87),
    'minecraft:frosted_ice_0': (140, 181, 253),
    'minecraft:frosted_ice_1': (140, 181, 253),
    'minecraft:frosted_ice_2': (138, 180, 252),
    'minecraft:frosted_ice_3': (135, 178, 252),
    'minecraft:furnace_front': (92, 91, 91),
    'minecraft:furnace_front_on': (121, 113, 94),
    'minecraft:furnace_side': (121, 120, 120),
    'minecraft:furnace_top': (110, 110, 110),
    'minecraft:gilded_blackstone': (56, 43, 38),
    'minecraft:glass': (176, 214, 219),
    'minecraft:glass_pane_top': (170, 210, 217),
    'minecraft:glow_item_frame': (148, 103, 64),
    'minecraft:glow_lichen': (112, 131, 122),
    'minecraft:glowstone': (172, 131, 84),
    'minecraft:gold_block': (246, 208, 62),
    'minecraft:gold_ore': (145, 134, 107),
    'minecraft:granite': (149, 103, 86),
    'minecraft:grass_block_side': (127, 107, 66),
    'minecraft:grass_block_side_overlay': (155, 155, 155),
    'minecraft:grass_block_snow': (170, 151, 133),
    'minecraft:grass_block_top': (147, 147, 147),
    'minecraft:gravel': (132, 127, 127),
    'minecraft:gray_candle': (80, 94, 97),
    'minecraft:gray_candle_lit': (118, 122, 108),
    'minecraft:gray_concrete': (55, 58, 62),
    'minecraft:gray_concrete_powder': (77, 81, 85),
    'minecraft:gray_glazed_terracotta': (83, 90, 94),
    'minecraft:gray_shulker_box': (55, 59, 62),
    'minecraft:gray_stained_glass': (76, 76, 76),
    'minecraft:gray_stained_glass_pane_top': (74, 74, 74),
    'minecraft:gray_terracotta': (58, 42, 36),
    'minecraft:gray_wool': (63, 68, 72),
    'minecraft:green_candle': (73, 96, 21),
    'minecraft:green_candle_lit': (101, 115, 22),
    'minecraft:green_concrete': (73, 91, 36),
    'minecraft:green_concrete_powder': (97, 119, 45),
    'minecraft:green_glazed_terracotta': (117, 142, 67),
    'minecraft:green_shulker_box': (79, 101, 32),
    'minecraft:green_stained_glass': (102, 127, 51),
    'minecraft:green_stained_glass_pane_top': (98, 123, 49),
    'minecraft:green_terracotta': (76, 83, 42),
    'minecraft:green_wool': (85, 110, 28),
    'minecraft:grindstone_pivot': (73, 46, 21),
    'minecraft:grindstone_round': (142, 142, 142),
    'minecraft:grindstone_side': (140, 140, 140),
    'minecraft:hanging_roots': (161, 115, 92),
    'minecraft:hay_block_side': (166, 136, 38),
    'minecraft:hay_block_top': (166, 139, 12),
    'minecraft:heavy_core': (82, 86, 94),
    'minecraft:honey_block_bottom': (241, 146, 18),
    'minecraft:honey_block_side': (251, 188, 58),
    'minecraft:honey_block_top': (251, 185, 53),
    'minecraft:honeycomb_block': (229, 148, 30),
    'minecraft:hopper_inside': (49, 49, 53),
    'minecraft:hopper_outside': (67, 66, 68),
    'minecraft:hopper_top': (76, 74, 76),
    'minecraft:horn_coral': (209, 186, 63),
    'minecraft:horn_coral_block': (216, 200, 66),
    'minecraft:horn_coral_fan': (206, 183, 61),
    'minecraft:ice': (146, 184, 254),
    'minecraft:iron_bars': (137, 139, 136),
    'minecraft:iron_block': (220, 220, 220),
    'minecraft:iron_chain': (51, 58, 74),
    'minecraft:iron_door_bottom': (199, 178, 85),
    'minecraft:iron_door_top': (199, 178, 85),
    'minecraft:iron_ore': (136, 129, 123),
    'minecraft:iron_trapdoor': (203, 202, 202),
    'minecraft:item_frame': (117, 67, 43),
    'minecraft:jack_o_lantern': (215, 152, 53),
    'minecraft:jigsaw_bottom': (34, 27, 37),
    'minecraft:jigsaw_lock': (45, 38, 47),
    'minecraft:jigsaw_side': (62, 54, 63),
    'minecraft:jigsaw_top': (80, 70, 81),
    'minecraft:jukebox_side': (89, 59, 41),
    'minecraft:jukebox_top': (94, 64, 47),
    'minecraft:jungle_door_bottom': (199, 178, 85),
    'minecraft:jungle_door_top': (199, 178, 85),
    'minecraft:jungle_leaves': (157, 154, 144),
    'minecraft:jungle_log': (85, 68, 25),
    'minecraft:jungle_log_top': (150, 109, 71),
    'minecraft:jungle_planks': (160, 115, 81),
    'minecraft:jungle_sapling': (48, 81, 17),
    'minecraft:jungle_shelf': (156, 122, 74),
    'minecraft:jungle_trapdoor': (153, 110, 77),
    'minecraft:kelp': (87, 140, 45),
    'minecraft:kelp_plant': (87, 130, 43),
    'minecraft:ladder': (125, 97, 55),
    'minecraft:lantern': (106, 91, 84),
    'minecraft:lapis_block': (31, 67, 140),
    'minecraft:lapis_ore': (107, 118, 141),
    'minecraft:large_amethyst_bud': (161, 126, 203),
    'minecraft:large_fern_bottom': (132, 132, 132),
    'minecraft:large_fern_top': (125, 126, 125),
    'minecraft:lava_flow': (207, 92, 20),
    'minecraft:lava_still': (212, 90, 18),
    'minecraft:leaf_litter': (171, 171, 171),
    'minecraft:lectern_base': (163, 121, 74),
    'minecraft:lectern_front': (130, 101, 56),
    'minecraft:lectern_sides': (150, 117, 68),
    'minecraft:lectern_top': (174, 138, 83),
    'minecraft:lever': (111, 93, 67),
    'minecraft:light_blue_candle': (35, 138, 197),
    'minecraft:light_blue_candle_lit': (70, 162, 202),
    'minecraft:light_blue_concrete': (36, 137, 199),
    'minecraft:light_blue_concrete_powder': (74, 181, 213),
    'minecraft:light_blue_glazed_terracotta': (95, 165, 209),
    'minecraft:light_blue_shulker_box': (49, 164, 212),
    'minecraft:light_blue_stained_glass': (102, 153, 216),
    'minecraft:light_blue_stained_glass_pane_top': (98, 147, 209),
    'minecraft:light_blue_terracotta': (113, 109, 138),
    'minecraft:light_blue_wool': (58, 175, 217),
    'minecraft:light_gray_candle': (118, 121, 112),
    'minecraft:light_gray_candle_lit': (152, 147, 126),
    'minecraft:light_gray_concrete': (125, 125, 115),
    'minecraft:light_gray_concrete_powder': (155, 155, 148),
    'minecraft:light_gray_glazed_terracotta': (144, 166, 168),
    'minecraft:light_gray_shulker_box': (124, 124, 115),
    'minecraft:light_gray_stained_glass': (153, 153, 153),
    'minecraft:light_gray_stained_glass_pane_top': (147, 147, 147),
    'minecraft:light_gray_terracotta': (135, 107, 98),
    'minecraft:light_gray_wool': (142, 142, 135),
    'minecraft:lightning_rod': (197, 111, 83),
    'minecraft:lightning_rod_on': (255, 255, 255),
    'minecraft:lilac_bottom': (137, 124, 127),
    'minecraft:lilac_top': (155, 125, 147),
    'minecraft:lily_of_the_valley': (123, 175, 95),
    'minecraft:lily_pad': (134, 134, 134),
    'minecraft:lime_candle': (98, 172, 23),
    'minecraft:lime_candle_lit': (124, 181, 31),
    'minecraft:lime_concrete': (94, 169, 24),
    'minecraft:lime_concrete_powder': (125, 189, 42),
    'minecraft:lime_glazed_terracotta': (163, 198, 55),
    'minecraft:lime_shulker_box': (100, 173, 23),
    'minecraft:lime_stained_glass': (127, 204, 25),
    'minecraft:lime_stained_glass_pane_top': (123, 197, 24),
    'minecraft:lime_terracotta': (104, 118, 53),
    'minecraft:lime_wool': (112, 185, 26),
    'minecraft:lodestone_side': (119, 120, 123),
    'minecraft:lodestone_top': (147, 149, 153),
    'minecraft:loom_bottom': (76, 60, 36),
    'minecraft:loom_front': (148, 119, 82),
    'minecraft:loom_side': (146, 101, 72),
    'minecraft:loom_top': (142, 119, 92),
    'minecraft:magenta_candle': (161, 46, 153),
    'minecraft:magenta_candle_lit': (182, 70, 162),
    'minecraft:magenta_concrete': (169, 48, 159),
    'minecraft:magenta_concrete_powder': (193, 84, 185),
    'minecraft:magenta_glazed_terracotta': (208, 100, 192),
    'minecraft:magenta_shulker_box': (174, 54, 164),
    'minecraft:magenta_stained_glass': (178, 76, 216),
    'minecraft:magenta_stained_glass_pane_top': (171, 74, 209),
    'minecraft:magenta_terracotta': (150, 88, 109),
    'minecraft:magenta_wool': (190, 69, 180),
    'minecraft:magma': (143, 63, 32),
    'minecraft:mangrove_door_bottom': (199, 178, 85),
    'minecraft:mangrove_door_top': (199, 178, 85),
    'minecraft:mangrove_leaves': (130, 129, 129),
    'minecraft:mangrove_log': (84, 67, 41),
    'minecraft:mangrove_log_top': (103, 49, 42),
    'minecraft:mangrove_planks': (118, 54, 49),
    'minecraft:mangrove_propagule': (96, 175, 84),
    'minecraft:mangrove_propagule_hanging': (109, 135, 70),
    'minecraft:mangrove_roots_side': (75, 60, 38),
    'minecraft:mangrove_roots_top': (75, 60, 39),
    'minecraft:mangrove_shelf': (109, 45, 42),
    'minecraft:mangrove_trapdoor': (111, 47, 42),
    'minecraft:medium_amethyst_bud': (158, 120, 202),
    'minecraft:melon_side': (114, 146, 30),
    'minecraft:melon_stem': (154, 154, 154),
    'minecraft:melon_top': (111, 145, 31),
    'minecraft:moss_block': (89, 110, 45),
    'minecraft:mossy_cobblestone': (110, 118, 95),
    'minecraft:mossy_stone_bricks': (115, 121, 105),
    'minecraft:mud': (60, 57, 61),
    'minecraft:mud_bricks': (137, 104, 79),
    'minecraft:muddy_mangrove_roots_side': (68, 59, 48),
    'minecraft:muddy_mangrove_roots_top': (70, 59, 45),
    'minecraft:mushroom_block_inside': (202, 170, 120),
    'minecraft:mushroom_stem': (203, 197, 186),
    'minecraft:mycelium_side': (114, 88, 72),
    'minecraft:mycelium_top': (111, 99, 101),
    'minecraft:nether_bricks': (44, 22, 26),
    'minecraft:nether_gold_ore': (115, 55, 42),
    'minecraft:nether_portal': (87, 11, 191),
    'minecraft:nether_quartz_ore': (118, 66, 62),
    'minecraft:nether_sprouts': (20, 151, 133),
    'minecraft:nether_wart_block': (115, 3, 2),
    'minecraft:nether_wart_stage0': (118, 19, 22),
    'minecraft:nether_wart_stage1': (116, 17, 19),
    'minecraft:nether_wart_stage2': (111, 18, 19),
    'minecraft:netherite_block': (67, 61, 64),
    'minecraft:netherrack': (98, 38, 38),
    'minecraft:note_block': (89, 59, 41),
    'minecraft:oak_door_bottom': (199, 178, 85),
    'minecraft:oak_door_top': (199, 178, 85),
    'minecraft:oak_leaves': (144, 144, 144),
    'minecraft:oak_log': (109, 85, 51),
    'minecraft:oak_log_top': (151, 122, 73),
    'minecraft:oak_planks': (162, 131, 79),
    'minecraft:oak_sapling': (78, 107, 41),
    'minecraft:oak_shelf': (145, 115, 67),
    'minecraft:oak_trapdoor': (125, 99, 57),
    'minecraft:observer_back': (72, 70, 70),
    'minecraft:observer_back_on': (76, 68, 68),
    'minecraft:observer_front': (104, 103, 103),
    'minecraft:observer_side': (70, 69, 69),
    'minecraft:observer_top': (98, 98, 98),
    'minecraft:obsidian': (15, 11, 25),
    'minecraft:ochre_froglight_side': (245, 233, 182),
    'minecraft:ochre_froglight_top': (251, 245, 206),
    'minecraft:open_eyeblossom': (133, 125, 128),
    'minecraft:open_eyeblossom_emissive': (238, 138, 38),
    'minecraft:orange_candle': (219, 100, 9),
    'minecraft:orange_candle_lit': (227, 131, 27),
    'minecraft:orange_concrete': (224, 97, 1),
    'minecraft:orange_concrete_powder': (227, 132, 32),
    'minecraft:orange_glazed_terracotta': (155, 147, 92),
    'minecraft:orange_shulker_box': (234, 106, 9),
    'minecraft:orange_stained_glass': (216, 127, 51),
    'minecraft:orange_stained_glass_pane_top': (209, 123, 49),
    'minecraft:orange_terracotta': (162, 84, 38),
    'minecraft:orange_tulip': (93, 142, 31),
    'minecraft:orange_wool': (241, 118, 20),
    'minecraft:oxeye_daisy': (179, 202, 143),
    'minecraft:oxidized_chiseled_copper': (84, 162, 132),
    'minecraft:oxidized_copper': (82, 163, 133),
    'minecraft:oxidized_copper_bars': (64, 123, 101),
    'minecraft:oxidized_copper_bulb': (70, 132, 109),
    'minecraft:oxidized_copper_bulb_lit': (135, 154, 104),
    'minecraft:oxidized_copper_bulb_lit_powered': (136, 153, 103),
    'minecraft:oxidized_copper_bulb_powered': (72, 131, 108),
    'minecraft:oxidized_copper_chain': (62, 118, 99),
    'minecraft:oxidized_copper_door_bottom': (199, 178, 85),
    'minecraft:oxidized_copper_door_top': (199, 178, 85),
    'minecraft:oxidized_copper_grate': (82, 161, 131),
    'minecraft:oxidized_copper_lantern': (78, 139, 111),
    'minecraft:oxidized_copper_trapdoor': (84, 161, 132),
    'minecraft:oxidized_cut_copper': (80, 154, 126),
    'minecraft:oxidized_lightning_rod': (79, 154, 128),
    'minecraft:packed_ice': (142, 180, 250),
    'minecraft:packed_mud': (142, 107, 80),
    'minecraft:pale_hanging_moss': (99, 104, 98),
    'minecraft:pale_hanging_moss_tip': (103, 108, 102),
    'minecraft:pale_moss_block': (107, 112, 105),
    'minecraft:pale_moss_carpet': (107, 112, 105),
    'minecraft:pale_moss_carpet_side_small': (112, 118, 110),
    'minecraft:pale_moss_carpet_side_tall': (103, 108, 102),
    'minecraft:pale_oak_door_bottom': (199, 178, 85),
    'minecraft:pale_oak_door_top': (199, 178, 85),
    'minecraft:pale_oak_leaves': (224, 216, 211),
    'minecraft:pale_oak_log': (124, 114, 111),
    'minecraft:pale_oak_log_top': (199, 189, 188),
    'minecraft:pale_oak_planks': (251, 239, 236),
    'minecraft:pale_oak_sapling': (110, 106, 100),
    'minecraft:pale_oak_shelf': (251, 239, 236),
    'minecraft:pale_oak_trapdoor': (251, 239, 236),
    'minecraft:pearlescent_froglight_side': (236, 225, 229),
    'minecraft:pearlescent_froglight_top': (246, 240, 240),
    'minecraft:peony_bottom': (87, 101, 94),
    'minecraft:peony_top': (130, 127, 139),
    'minecraft:pink_candle': (208, 102, 142),
    'minecraft:pink_candle_lit': (218, 131, 151),
    'minecraft:pink_concrete': (214, 101, 143),
    'minecraft:pink_concrete_powder': (229, 153, 181),
    'minecraft:pink_glazed_terracotta': (235, 155, 182),
    'minecraft:pink_petals': (247, 181, 219),
    'minecraft:pink_petals_stem': (169, 169, 169),
    'minecraft:pink_shulker_box': (230, 122, 158),
    'minecraft:pink_stained_glass': (242, 127, 165),
    'minecraft:pink_stained_glass_pane_top': (233, 123, 160),
    'minecraft:pink_terracotta': (162, 78, 79),
    'minecraft:pink_tulip': (99, 157, 78),
    'minecraft:pink_wool': (238, 141, 172),
    'minecraft:piston_bottom': (97, 97, 97),
    'minecraft:piston_inner': (97, 97, 97),
    'minecraft:piston_side': (110, 105, 97),
    'minecraft:piston_top': (153, 128, 85),
    'minecraft:piston_top_sticky': (123, 149, 92),
    'minecraft:pitcher_crop_bottom': (100, 58, 37),
    'minecraft:pitcher_crop_bottom_stage_1': (142, 155, 87),
    'minecraft:pitcher_crop_bottom_stage_2': (115, 129, 80),
    'minecraft:pitcher_crop_bottom_stage_3': (107, 125, 75),
    'minecraft:pitcher_crop_bottom_stage_4': (104, 126, 93),
    'minecraft:pitcher_crop_side': (178, 126, 81),
    'minecraft:pitcher_crop_top': (194, 166, 103),
    'minecraft:pitcher_crop_top_stage_3': (143, 185, 138),
    'minecraft:pitcher_crop_top_stage_4': (122, 144, 189),
    'minecraft:podzol_side': (123, 88, 57),
    'minecraft:podzol_top': (92, 63, 24),
    'minecraft:pointed_dripstone_down_base': (129, 103, 90),
    'minecraft:pointed_dripstone_down_frustum': (129, 103, 89),
    'minecraft:pointed_dripstone_down_middle': (130, 104, 90),
    'minecraft:pointed_dripstone_down_tip': (137, 109, 94),
    'minecraft:pointed_dripstone_down_tip_merge': (139, 112, 95),
    'minecraft:pointed_dripstone_up_base': (129, 103, 90),
    'minecraft:pointed_dripstone_up_frustum': (129, 103, 89),
    'minecraft:pointed_dripstone_up_middle': (130, 104, 90),
    'minecraft:pointed_dripstone_up_tip': (137, 109, 94),
    'minecraft:pointed_dripstone_up_tip_merge': (139, 112, 95),
    'minecraft:polished_andesite': (132, 135, 134),
    'minecraft:polished_basalt_side': (89, 88, 92),
    'minecraft:polished_basalt_top': (99, 99, 101),
    'minecraft:polished_blackstone': (53, 49, 57),
    'minecraft:polished_blackstone_bricks': (48, 43, 50),
    'minecraft:polished_deepslate': (72, 73, 73),
    'minecraft:polished_diorite': (193, 193, 195),
    'minecraft:polished_granite': (154, 107, 89),
    'minecraft:polished_tuff': (98, 104, 100),
    'minecraft:poppy': (129, 65, 38),
    'minecraft:potatoes_stage0': (59, 130, 41),
    'minecraft:potatoes_stage1': (69, 132, 43),
    'minecraft:potatoes_stage2': (86, 129, 48),
    'minecraft:potatoes_stage3': (85, 135, 47),
    'minecraft:potted_azalea_bush_plant': (93, 108, 43),
    'minecraft:potted_azalea_bush_side': (96, 119, 47),
    'minecraft:potted_azalea_bush_top': (101, 123, 48),
    'minecraft:potted_flowering_azalea_bush_plant': (100, 104, 56),
    'minecraft:potted_flowering_azalea_bush_side': (110, 114, 70),
    'minecraft:potted_flowering_azalea_bush_top': (115, 120, 70),
    'minecraft:powder_snow': (248, 253, 253),
    'minecraft:powered_rail': (138, 110, 74),
    'minecraft:powered_rail_on': (154, 110, 74),
    'minecraft:prismarine': (99, 156, 151),
    'minecraft:prismarine_bricks': (99, 172, 158),
    'minecraft:pumpkin_side': (196, 115, 24),
    'minecraft:pumpkin_stem': (154, 154, 154),
    'minecraft:pumpkin_top': (198, 119, 24),
    'minecraft:purple_candle': (105, 34, 159),
    'minecraft:purple_candle_lit': (125, 34, 145),
    'minecraft:purple_concrete': (100, 32, 156),
    'minecraft:purple_concrete_powder': (132, 56, 178),
    'minecraft:purple_glazed_terracotta': (110, 48, 152),
    'minecraft:purple_shulker_box': (103, 32, 156),
    'minecraft:purple_stained_glass': (127, 63, 178),
    'minecraft:purple_stained_glass_pane_top': (123, 61, 171),
    'minecraft:purple_terracotta': (118, 70, 86),
    'minecraft:purple_wool': (122, 42, 173),
    'minecraft:purpur_block': (170, 126, 170),
    'minecraft:purpur_pillar': (172, 130, 172),
    'minecraft:purpur_pillar_top': (172, 128, 171),
    'minecraft:quartz_block_bottom': (237, 230, 224),
    'minecraft:quartz_block_side': (236, 230, 223),
    'minecraft:quartz_block_top': (236, 230, 223),
    'minecraft:quartz_bricks': (235, 229, 222),
    'minecraft:quartz_pillar': (236, 231, 224),
    'minecraft:quartz_pillar_top': (235, 230, 223),
    'minecraft:rail': (126, 112, 89),
    'minecraft:rail_corner': (130, 115, 90),
    'minecraft:raw_copper_block': (154, 106, 79),
    'minecraft:raw_gold_block': (222, 169, 47),
    'minecraft:raw_iron_block': (166, 136, 107),
    'minecraft:red_candle': (153, 39, 36),
    'minecraft:red_candle_lit': (181, 65, 47),
    'minecraft:red_concrete': (142, 33, 33),
    'minecraft:red_concrete_powder': (168, 54, 51),
    'minecraft:red_glazed_terracotta': (182, 60, 53),
    'minecraft:red_mushroom': (217, 75, 68),
    'minecraft:red_mushroom_block': (200, 47, 45),
    'minecraft:red_nether_bricks': (70, 7, 9),
    'minecraft:red_sand': (191, 103, 33),
    'minecraft:red_sandstone': (187, 99, 29),
    'minecraft:red_sandstone_bottom': (186, 98, 28),
    'minecraft:red_sandstone_top': (181, 98, 31),
    'minecraft:red_shulker_box': (140, 31, 30),
    'minecraft:red_stained_glass': (153, 51, 51),
    'minecraft:red_stained_glass_pane_top': (147, 49, 49),
    'minecraft:red_terracotta': (143, 61, 47),
    'minecraft:red_tulip': (90, 129, 33),
    'minecraft:red_wool': (161, 39, 35),
    'minecraft:redstone_block': (176, 25, 5),
    'minecraft:redstone_dust_dot': (240, 240, 240),
    'minecraft:redstone_dust_line0': (240, 240, 240),
    'minecraft:redstone_dust_line1': (240, 240, 240),
    'minecraft:redstone_dust_overlay': (0, 0, 0),
    'minecraft:redstone_lamp': (95, 55, 30),
    'minecraft:redstone_lamp_on': (143, 102, 61),
    'minecraft:redstone_ore': (140, 110, 110),
    'minecraft:redstone_torch': (182, 59, 44),
    'minecraft:redstone_torch_off': (101, 70, 44),
    'minecraft:reinforced_deepslate_bottom': (79, 82, 80),
    'minecraft:reinforced_deepslate_side': (102, 109, 101),
    'minecraft:reinforced_deepslate_top': (80, 83, 79),
    'minecraft:repeater': (160, 157, 156),
    'minecraft:repeater_on': (169, 157, 156),
    'minecraft:repeating_command_block_back': (128, 110, 167),
    'minecraft:repeating_command_block_conditional': (127, 109, 172),
    'minecraft:repeating_command_block_front': (129, 111, 176),
    'minecraft:repeating_command_block_side': (129, 110, 171),
    'minecraft:resin_block': (217, 99, 25),
    'minecraft:resin_bricks': (206, 88, 24),
    'minecraft:resin_clump': (223, 112, 27),
    'minecraft:respawn_anchor_bottom': (33, 10, 60),
    'minecraft:respawn_anchor_side0': (40, 24, 63),
    'minecraft:respawn_anchor_side1': (42, 27, 65),
    'minecraft:respawn_anchor_side2': (45, 28, 66),
    'minecraft:respawn_anchor_side3': (47, 30, 67),
    'minecraft:respawn_anchor_side4': (50, 32, 68),
    'minecraft:respawn_anchor_top': (76, 24, 150),
    'minecraft:respawn_anchor_top_off': (34, 22, 52),
    'minecraft:rooted_dirt': (144, 104, 77),
    'minecraft:rose_bush_bottom': (98, 84, 38),
    'minecraft:rose_bush_top': (131, 66, 37),
    'minecraft:sand': (219, 207, 163),
    'minecraft:sandstone': (216, 203, 156),
    'minecraft:sandstone_bottom': (216, 202, 154),
    'minecraft:sandstone_top': (224, 214, 170),
    'minecraft:scaffolding_bottom': (194, 173, 80),
    'minecraft:scaffolding_side': (193, 170, 79),
    'minecraft:scaffolding_top': (170, 132, 73),
    'minecraft:sculk': (13, 30, 36),
    'minecraft:sculk_catalyst_bottom': (89, 109, 109),
    'minecraft:sculk_catalyst_side': (77, 94, 90),
    'minecraft:sculk_catalyst_side_bloom': (77, 97, 93),
    'minecraft:sculk_catalyst_top': (15, 32, 38),
    'minecraft:sculk_catalyst_top_bloom': (16, 45, 51),
    'minecraft:sculk_sensor_bottom': (13, 28, 34),
    'minecraft:sculk_sensor_side': (10, 39, 47),
    'minecraft:sculk_sensor_tendril_active': (9, 123, 127),
    'minecraft:sculk_sensor_tendril_inactive': (15, 66, 77),
    'minecraft:sculk_sensor_top': (7, 70, 84),
    'minecraft:sculk_shrieker_bottom': (13, 28, 34),
    'minecraft:sculk_shrieker_can_summon_inner_top': (31, 65, 66),
    'minecraft:sculk_shrieker_inner_top': (30, 54, 55),
    'minecraft:sculk_shrieker_side': (75, 101, 95),
    'minecraft:sculk_shrieker_top': (199, 205, 170),
    'minecraft:sculk_vein': (8, 48, 58),
    'minecraft:sea_lantern': (172, 200, 190),
    'minecraft:sea_pickle': (90, 97, 40),
    'minecraft:seagrass': (51, 127, 8),
    'minecraft:short_dry_grass': (187, 159, 108),
    'minecraft:short_grass': (146, 145, 146),
    'minecraft:shroomlight': (241, 147, 71),
    'minecraft:shulker_box': (139, 97, 139),
    'minecraft:slime_block': (112, 192, 92),
    'minecraft:small_amethyst_bud': (132, 99, 192),
    'minecraft:small_dripleaf_side': (108, 128, 49),
    'minecraft:small_dripleaf_stem_bottom': (95, 122, 45),
    'minecraft:small_dripleaf_stem_top': (97, 121, 46),
    'minecraft:small_dripleaf_top': (95, 120, 46),
    'minecraft:smithing_table_bottom': (64, 28, 24),
    'minecraft:smithing_table_front': (57, 37, 39),
    'minecraft:smithing_table_side': (55, 35, 36),
    'minecraft:smithing_table_top': (57, 59, 71),
    'minecraft:smoker_bottom': (107, 106, 104),
    'minecraft:smoker_front': (88, 75, 58),
    'minecraft:smoker_front_on': (119, 97, 67),
    'minecraft:smoker_side': (103, 92, 76),
    'minecraft:smoker_top': (85, 84, 81),
    'minecraft:smooth_basalt': (73, 72, 78),
    'minecraft:smooth_stone': (159, 159, 159),
    'minecraft:smooth_stone_slab_side': (168, 168, 168),
    'minecraft:sniffer_egg_not_cracked_bottom': (73, 25, 26),
    'minecraft:sniffer_egg_not_cracked_east': (94, 73, 49),
    'minecraft:sniffer_egg_not_cracked_north': (92, 77, 51),
    'minecraft:sniffer_egg_not_cracked_south': (93, 78, 52),
    'minecraft:sniffer_egg_not_cracked_top': (135, 105, 68),
    'minecraft:sniffer_egg_not_cracked_west': (90, 79, 52),
    'minecraft:sniffer_egg_slightly_cracked_bottom': (71, 25, 26),
    'minecraft:sniffer_egg_slightly_cracked_east': (90, 68, 47),
    'minecraft:sniffer_egg_slightly_cracked_north': (89, 72, 49),
    'minecraft:sniffer_egg_slightly_cracked_south': (90, 73, 50),
    'minecraft:sniffer_egg_slightly_cracked_top': (129, 97, 62),
    'minecraft:sniffer_egg_slightly_cracked_west': (89, 76, 51),
    'minecraft:sniffer_egg_very_cracked_bottom': (70, 25, 26),
    'minecraft:sniffer_egg_very_cracked_east': (86, 65, 46),
    'minecraft:sniffer_egg_very_cracked_north': (86, 68, 47),
    'minecraft:sniffer_egg_very_cracked_south': (85, 69, 48),
    'minecraft:sniffer_egg_very_cracked_top': (120, 89, 58),
    'minecraft:sniffer_egg_very_cracked_west': (83, 71, 49),
    'minecraft:snow': (249, 254, 254),
    'minecraft:soul_campfire_fire': (81, 205, 208),
    'minecraft:soul_campfire_log_lit': (70, 107, 105),
    'minecraft:soul_fire_0': (51, 193, 197),
    'minecraft:soul_fire_1': (55, 193, 198),
    'minecraft:soul_lantern': (72, 99, 115),
    'minecraft:soul_sand': (81, 62, 51),
    'minecraft:soul_soil': (76, 58, 47),
    'minecraft:soul_torch': (109, 115, 90),
    'minecraft:spawner': (36, 46, 63),
    'minecraft:sponge': (196, 192, 75),
    'minecraft:spore_blossom': (207, 97, 159),
    'minecraft:spore_blossom_base': (113, 142, 50),
    'minecraft:spruce_door_bottom': (199, 178, 85),
    'minecraft:spruce_door_top': (199, 178, 85),
    'minecraft:spruce_leaves': (126, 126, 126),
    'minecraft:spruce_log': (59, 38, 17),
    'minecraft:spruce_log_top': (109, 80, 47),
    'minecraft:spruce_planks': (115, 85, 49),
    'minecraft:spruce_sapling': (45, 60, 37),
    'minecraft:spruce_shelf': (104, 83, 48),
    'minecraft:spruce_trapdoor': (104, 79, 48),
    'minecraft:stone': (126, 126, 126),
    'minecraft:stone_bricks': (122, 122, 122),
    'minecraft:stonecutter_bottom': (118, 118, 118),
    'minecraft:stonecutter_saw': (222, 222, 222),
    'minecraft:stonecutter_side': (107, 90, 78),
    'minecraft:stonecutter_top': (123, 119, 111),
    'minecraft:stripped_acacia_log': (175, 93, 60),
    'minecraft:stripped_acacia_log_top': (166, 91, 52),
    'minecraft:stripped_bamboo_block': (193, 173, 80),
    'minecraft:stripped_bamboo_block_top': (178, 159, 73),
    'minecraft:stripped_birch_log': (197, 176, 118),
    'minecraft:stripped_birch_log_top': (191, 172, 116),
    'minecraft:stripped_cherry_log': (215, 145, 149),
    'minecraft:stripped_cherry_log_top': (221, 165, 158),
    'minecraft:stripped_crimson_stem': (137, 57, 90),
    'minecraft:stripped_crimson_stem_top': (122, 56, 83),
    'minecraft:stripped_dark_oak_log': (73, 57, 36),
    'minecraft:stripped_dark_oak_log_top': (66, 44, 23),
    'minecraft:stripped_jungle_log': (171, 133, 85),
    'minecraft:stripped_jungle_log_top': (166, 123, 82),
    'minecraft:stripped_mangrove_log': (120, 54, 48),
    'minecraft:stripped_mangrove_log_top': (109, 44, 43),
    'minecraft:stripped_oak_log': (177, 144, 86),
    'minecraft:stripped_oak_log_top': (160, 130, 77),
    'minecraft:stripped_pale_oak_log': (251, 239, 236),
    'minecraft:stripped_pale_oak_log_top': (235, 227, 226),
    'minecraft:stripped_spruce_log': (116, 90, 52),
    'minecraft:stripped_spruce_log_top': (106, 80, 47),
    'minecraft:stripped_warped_stem': (58, 151, 148),
    'minecraft:stripped_warped_stem_top': (52, 129, 124),
    'minecraft:structure_block': (89, 74, 90),
    'minecraft:structure_block_corner': (68, 58, 70),
    'minecraft:structure_block_data': (79, 66, 81),
    'minecraft:structure_block_load': (69, 58, 71),
    'minecraft:structure_block_save': (86, 72, 88),
    'minecraft:sugar_cane': (149, 193, 101),
    'minecraft:sunflower_back': (55, 128, 35),
    'minecraft:sunflower_bottom': (57, 135, 31),
    'minecraft:sunflower_front': (246, 197, 54),
    'minecraft:sunflower_top': (50, 129, 27),
    'minecraft:suspicious_gravel_0': (130, 125, 124),
    'minecraft:suspicious_gravel_1': (129, 124, 123),
    'minecraft:suspicious_gravel_2': (126, 121, 120),
    'minecraft:suspicious_gravel_3': (124, 119, 118),
    'minecraft:suspicious_sand_0': (218, 204, 159),
    'minecraft:suspicious_sand_1': (217, 203, 157),
    'minecraft:suspicious_sand_2': (214, 199, 151),
    'minecraft:suspicious_sand_3': (211, 193, 145),
    'minecraft:sweet_berry_bush_stage0': (43, 90, 56),
    'minecraft:sweet_berry_bush_stage1': (48, 94, 58),
    'minecraft:sweet_berry_bush_stage2': (60, 88, 56),
    'minecraft:sweet_berry_bush_stage3': (68, 78, 51),
    'minecraft:tall_dry_grass': (197, 172, 123),
    'minecraft:tall_grass_bottom': (128, 128, 128),
    'minecraft:tall_grass_top': (151, 149, 151),
    'minecraft:tall_seagrass_bottom': (45, 117, 4),
    'minecraft:tall_seagrass_top': (59, 139, 14),
    'minecraft:target_side': (229, 176, 168),
    'minecraft:target_top': (226, 170, 158),
    'minecraft:terracotta': (152, 94, 68),
    'minecraft:test_block_accept': (132, 191, 117),
    'minecraft:test_block_fail': (125, 113, 171),
    'minecraft:test_block_log': (143, 176, 214),
    'minecraft:test_block_start': (191, 158, 120),
    'minecraft:test_instance_block': (127, 121, 119),
    'minecraft:tinted_glass': (44, 39, 46),
    'minecraft:tnt_bottom': (167, 67, 53),
    'minecraft:tnt_side': (182, 88, 84),
    'minecraft:tnt_top': (143, 62, 54),
    'minecraft:torch': (139, 113, 64),
    'minecraft:torchflower': (101, 101, 77),
    'minecraft:torchflower_crop_stage0': (29, 98, 67),
    'minecraft:torchflower_crop_stage1': (65, 109, 76),
    'minecraft:trial_spawner_bottom': (42, 59, 76),
    'minecraft:trial_spawner_side_active': (54, 66, 77),
    'minecraft:trial_spawner_side_active_ominous': (46, 66, 82),
    'minecraft:trial_spawner_side_inactive': (48, 68, 83),
    'minecraft:trial_spawner_side_inactive_ominous': (40, 58, 74),
    'minecraft:trial_spawner_top_active': (63, 81, 92),
    'minecraft:trial_spawner_top_active_ominous': (49, 84, 100),
    'minecraft:trial_spawner_top_ejecting_reward': (47, 53, 62),
    'minecraft:trial_spawner_top_ejecting_reward_ominous': (33, 56, 70),
    'minecraft:trial_spawner_top_inactive': (57, 83, 99),
    'minecraft:trial_spawner_top_inactive_ominous': (49, 74, 91),
    'minecraft:tripwire': (129, 129, 129),
    'minecraft:tripwire_hook': (143, 133, 118),
    'minecraft:tube_coral': (48, 83, 197),
    'minecraft:tube_coral_block': (49, 87, 207),
    'minecraft:tube_coral_fan': (51, 92, 209),
    'minecraft:tuff': (108, 109, 103),
    'minecraft:tuff_bricks': (98, 103, 95),
    'minecraft:turtle_egg': (228, 227, 192),
    'minecraft:turtle_egg_slightly_cracked': (218, 215, 178),
    'minecraft:turtle_egg_very_cracked': (208, 204, 165),
    'minecraft:twisting_vines': (20, 143, 124),
    'minecraft:twisting_vines_plant': (20, 136, 122),
    'minecraft:vault_bottom': (44, 43, 56),
    'minecraft:vault_bottom_ominous': (44, 43, 56),
    'minecraft:vault_front_ejecting': (60, 64, 66),
    'minecraft:vault_front_ejecting_ominous': (50, 67, 72),
    'minecraft:vault_front_off': (51, 66, 74),
    'minecraft:vault_front_off_ominous': (56, 65, 70),
    'minecraft:vault_front_on': (60, 63, 66),
    'minecraft:vault_front_on_ominous': (52, 68, 72),
    'minecraft:vault_side_off': (55, 69, 75),
    'minecraft:vault_side_off_ominous': (55, 69, 75),
    'minecraft:vault_side_on': (63, 67, 66),
    'minecraft:vault_side_on_ominous': (46, 71, 76),
    'minecraft:vault_top': (55, 70, 79),
    'minecraft:vault_top_ejecting': (43, 47, 55),
    'minecraft:vault_top_ejecting_ominous': (45, 47, 51),
    'minecraft:vault_top_ominous': (69, 73, 71),
    'minecraft:verdant_froglight_side': (211, 235, 208),
    'minecraft:verdant_froglight_top': (229, 244, 228),
    'minecraft:vine': (116, 116, 116),
    'minecraft:warped_door_bottom': (199, 178, 85),
    'minecraft:warped_door_top': (199, 178, 85),
    'minecraft:warped_fungus': (74, 109, 88),
    'minecraft:warped_nylium': (43, 114, 101),
    'minecraft:warped_nylium_side': (73, 62, 60),
    'minecraft:warped_planks': (43, 105, 99),
    'minecraft:warped_roots': (20, 138, 124),
    'minecraft:warped_roots_pot': (21, 137, 123),
    'minecraft:warped_shelf': (51, 141, 134),
    'minecraft:warped_stem': (58, 59, 78),
    'minecraft:warped_stem_top': (53, 110, 110),
    'minecraft:warped_trapdoor': (47, 120, 112),
    'minecraft:warped_wart_block': (23, 120, 121),
    'minecraft:water_flow': (169, 169, 169),
    'minecraft:water_overlay': (165, 165, 165),
    'minecraft:water_still': (177, 177, 177),
    'minecraft:weathered_chiseled_copper': (105, 151, 111),
    'minecraft:weathered_copper': (108, 153, 110),
    'minecraft:weathered_copper_bars': (85, 123, 94),
    'minecraft:weathered_copper_bulb': (92, 127, 99),
    'minecraft:weathered_copper_bulb_lit': (156, 157, 99),
    'minecraft:weathered_copper_bulb_lit_powered': (158, 156, 98),
    'minecraft:weathered_copper_bulb_powered': (94, 125, 98),
    'minecraft:weathered_copper_chain': (73, 109, 92),
    'minecraft:weathered_copper_door_bottom': (199, 178, 85),
    'minecraft:weathered_copper_door_top': (199, 178, 85),
    'minecraft:weathered_copper_grate': (106, 153, 111),
    'minecraft:weathered_copper_lantern': (96, 134, 100),
    'minecraft:weathered_copper_trapdoor': (109, 153, 110),
    'minecraft:weathered_cut_copper': (109, 145, 108),
    'minecraft:weathered_lightning_rod': (97, 142, 105),
    'minecraft:weeping_vines': (105, 1, 0),
    'minecraft:weeping_vines_plant': (133, 16, 12),
    'minecraft:wet_sponge': (171, 181, 70),
    'minecraft:wheat_stage0': (9, 128, 16),
    'minecraft:wheat_stage1': (6, 128, 8),
    'minecraft:wheat_stage2': (7, 133, 13),
    'minecraft:wheat_stage3': (11, 131, 13),
    'minecraft:wheat_stage4': (37, 133, 18),
    'minecraft:wheat_stage5': (64, 129, 19),
    'minecraft:wheat_stage6': (142, 133, 37),
    'minecraft:wheat_stage7': (167, 152, 73),
    'minecraft:white_candle': (210, 217, 218),
    'minecraft:white_candle_lit': (218, 217, 206),
    'minecraft:white_concrete': (207, 213, 214),
    'minecraft:white_concrete_powder': (226, 227, 228),
    'minecraft:white_glazed_terracotta': (188, 212, 203),
    'minecraft:white_shulker_box': (216, 221, 221),
    'minecraft:white_stained_glass': (255, 255, 255),
    'minecraft:white_stained_glass_pane_top': (246, 246, 246),
    'minecraft:white_terracotta': (210, 178, 161),
    'minecraft:white_tulip': (94, 165, 71),
    'minecraft:white_wool': (234, 236, 237),
    'minecraft:wildflowers': (237, 214, 117),
    'minecraft:wildflowers_stem': (169, 169, 169),
    'minecraft:wither_rose': (41, 45, 23),
    # Legacy block-ID compatibility keys (pre-v1.6 block names)
    'minecraft:ancient_debris': (110, 80, 70),
    'minecraft:barrel': (130, 95, 60),
    'minecraft:blackstone_wall': (38, 38, 40),
    'minecraft:chest': (150, 110, 65),
    'minecraft:crafting_table': (140, 105, 70),
    'minecraft:dirt_path': (148, 122, 65),
    'minecraft:furnace': (110, 110, 110),
    'minecraft:grass': (106, 170, 64),
    'minecraft:grass_block': (106, 170, 64),
    'minecraft:large_fern': (88, 150, 62),
    'minecraft:lava': (255, 80, 0),
    'minecraft:moss_carpet': (90, 160, 75),
    'minecraft:mycelium': (120, 90, 120),
    'minecraft:podzol': (122, 102, 62),
    'minecraft:scaffolding': (190, 170, 120),
    'minecraft:slab': (140, 130, 120),
    'minecraft:snow_block': (240, 240, 240),
    'minecraft:stairs': (140, 130, 120),
    'minecraft:tall_grass': (106, 170, 64),
    'minecraft:trapdoor': (150, 120, 80),
    'minecraft:wall_torch': (245, 200, 80),
    'minecraft:water': (64, 64, 255),
    'minecraft:wheat': (167, 152, 73),
    'minecraft:yellow_candle': (209, 165, 50),
    'minecraft:yellow_candle_lit': (217, 184, 75),
    'minecraft:yellow_concrete': (241, 175, 21),
    'minecraft:yellow_concrete_powder': (233, 199, 55),
    'minecraft:yellow_glazed_terracotta': (234, 192, 89),
    'minecraft:yellow_shulker_box': (248, 189, 29),
    'minecraft:yellow_stained_glass': (229, 229, 51),
    'minecraft:yellow_stained_glass_pane_top': (221, 221, 49),
    'minecraft:yellow_terracotta': (186, 133, 35),
    'minecraft:yellow_wool': (249, 198, 40),
}
# Always build palette lookup tables at module load to avoid NameError in subprocesses
PALETTE_KEY_TO_IDX, PALETTE_COLOR_TABLE = build_palette_lookup(PALETTE)


def _avg_rgb(values: List[Tuple[int, int, int]]) -> Optional[Tuple[int, int, int]]:
    if not values:
        return None
    n = len(values)
    r = int(sum(v[0] for v in values) / n)
    g = int(sum(v[1] for v in values) / n)
    b = int(sum(v[2] for v in values) / n)
    return (r, g, b)


_GENERIC_SUFFIX_FAMILIES: Dict[str, str] = {
    "minecraft:fence": "_fence",
    "minecraft:fence_gate": "_fence_gate",
    "minecraft:wall": "_wall",
    "minecraft:button": "_button",
    "minecraft:pressure_plate": "_pressure_plate",
    "minecraft:door": "_door_bottom",
    "minecraft:trapdoor": "_trapdoor",
    "minecraft:sign": "_sign",
    "minecraft:wall_sign": "_wall_sign",
    "minecraft:wall_hanging_sign": "_hanging_sign",
    "minecraft:wall_banner": "_wall_banner",
    "minecraft:bed": "_bed_top",
    "minecraft:wood": "_wood",
}


_GENERIC_DIRECT_FALLBACKS: Dict[str, Tuple[int, int, int]] = {
    # Generic IDs commonly produced by universal mappings and caches.
    "minecraft:bamboo": (122, 168, 84),
    "minecraft:tall_seagrass": (50, 140, 120),
    "minecraft:melon": (118, 170, 62),
    "minecraft:mangrove_roots": (92, 78, 62),
    "minecraft:pumpkin": (196, 126, 46),
    "minecraft:cactus": (78, 136, 62),
    "minecraft:potatoes": (133, 152, 74),
    "minecraft:bubble_column": (64, 64, 255),
    "minecraft:beetroots": (129, 94, 70),
    "minecraft:carrots": (189, 128, 56),
    "minecraft:hay_block": (192, 170, 72),
    "minecraft:magma_block": (172, 78, 42),
    "minecraft:sweet_berry_bush": (132, 62, 58),
    "minecraft:composter": (130, 96, 62),
    "minecraft:bars": (150, 150, 150),
    "minecraft:coral": (186, 114, 132),
    "minecraft:button": (132, 108, 84),
    "minecraft:wall_banner": (154, 96, 78),
    "minecraft:wall_sign": (134, 94, 72),
    "minecraft:wall_hanging_sign": (134, 94, 72),
    "minecraft:pressure_plate": (136, 116, 90),
    "minecraft:bed": (166, 92, 92),
    "minecraft:sign": (134, 94, 72),
    "minecraft:carpet": (148, 126, 98),
    "minecraft:wood": (122, 94, 66),
    "minecraft:fire": (255, 98, 18),
    "minecraft:shelf": (140, 106, 72),
    "minecraft:hopper": (84, 84, 88),
    "minecraft:campfire": (126, 95, 64),
    "minecraft:azalea": (94, 166, 84),
    "minecraft:item_frame_block": (148, 106, 68),
    "minecraft:coral_fan": (189, 126, 148),
    "minecraft:small_dripleaf": (92, 152, 76),
    "minecraft:big_dripleaf": (92, 152, 76),
    "minecraft:ender_chest": (34, 28, 52),
    "minecraft:bee_nest": (184, 152, 80),
    "minecraft:head": (176, 140, 118),
    "minecraft:coral_block": (182, 112, 132),
    "minecraft:cocoa": (132, 88, 54),
    "minecraft:infested_block": (126, 126, 126),
    "minecraft:flowering_azalea": (100, 172, 92),
    "minecraft:sticky_piston_head": (132, 114, 94),
    "minecraft:decorated_pot": (158, 112, 80),
    "minecraft:waxed_exposed_chiseled_copper": (110, 150, 118),
    "minecraft:golem_statue": (156, 156, 156),
}


@lru_cache(maxsize=8192)
def _generic_palette_family_fallback(block_id: str) -> Optional[Tuple[int, int, int]]:
    """Best-effort color for generic IDs by reusing nearby palette families."""
    # First try stem/prefix variants like wheat_stage*, smoker_front, melon_side, etc.
    prefix = block_id + "_"
    prefix_hits = [rgb for key, rgb in PALETTE.items() if key.startswith(prefix)]
    avg = _avg_rgb(prefix_hits)
    if avg is not None:
        return avg

    # Then try known family suffixes for generic IDs like fence/wall/button/sign.
    suffix = _GENERIC_SUFFIX_FAMILIES.get(block_id)
    if suffix:
        suffix_hits = [rgb for key, rgb in PALETTE.items() if key.endswith(suffix)]
        avg = _avg_rgb(suffix_hits)
        if avg is not None:
            return avg

    return _GENERIC_DIRECT_FALLBACKS.get(block_id)


# Variant palettes (best-effort). If Amulet exposes variant properties (eg wood_type/color),
# we color those more accurately. If not, generic buckets still render non-gray.
VARIANT_PALETTE = {
    # Wood families
    "minecraft:wood|oak": (162, 130, 78),
    "minecraft:wood|spruce": (92, 66, 44),
    "minecraft:wood|birch": (200, 186, 120),
    "minecraft:wood|jungle": (154, 110, 77),
    "minecraft:wood|acacia": (170, 92, 60),
    "minecraft:wood|dark_oak": (72, 52, 34),
    "minecraft:wood|mangrove": (118, 46, 38),
    "minecraft:wood|cherry": (212, 140, 160),
    "minecraft:wood|bamboo": (200, 200, 90),
    "minecraft:wood|crimson": (130, 40, 60),
    "minecraft:wood|warped": (50, 140, 140),

    # Leaves families
    "minecraft:leaves|oak": (80, 150, 60),
    "minecraft:leaves|spruce": (60, 110, 50),
    "minecraft:leaves|birch": (95, 165, 65),
    "minecraft:leaves|jungle": (70, 140, 55),
    "minecraft:leaves|acacia": (95, 155, 60),
    "minecraft:leaves|dark_oak": (55, 105, 45),
    "minecraft:leaves|mangrove": (85, 135, 70),
    "minecraft:leaves|cherry": (214, 148, 176),
    "minecraft:leaves|bamboo": (100, 170, 70),
    "minecraft:leaves|crimson": (150, 60, 80),
    "minecraft:leaves|warped": (60, 170, 160),

    # Leaves (material-specific)
    "minecraft:leaves|pale_oak": (220, 214, 214),   # light grey with slight warm tint
    # Walls (material-specific)
    "minecraft:wall|blackstone": (38, 38, 40),
    "minecraft:wall|polished_blackstone": (43, 43, 46),
    "minecraft:wall|deepslate_bricks": (48, 49, 52),
    "minecraft:wall|deepslate_tiles": (46, 47, 50),

}

PLANT_TYPE_PALETTE = {
    # Common flowers / plants (best-effort)
    "grass": (96, 170, 72),
    "fern": (88, 160, 72),
    "seagrass": (50, 140, 120),
    "kelp": (45, 120, 110),
    "cornflower": (75, 110, 200),
    "dandelion": (230, 210, 60),
    "poppy": (200, 60, 60),
    "azure_bluet": (215, 215, 215),
    "oxeye_daisy": (235, 235, 235),
    "blue_orchid": (90, 130, 220),
    "allium": (175, 90, 190),
    "tulip_red": (200, 60, 60),
    "tulip_orange": (220, 120, 60),
    "tulip_white": (235, 235, 235),
    "tulip_pink": (230, 140, 170),
    "sunflower": (235, 205, 70),
    "lily_of_the_valley": (235, 235, 235),
    "pink_petals": (230, 150, 175),
    "wildflowers": (210, 200, 120),
}



COLOR_PALETTE = {
    "white": (235, 235, 235),
    "light_gray": (160, 160, 160),
    "gray": (95, 95, 95),
    "black": (30, 30, 30),
    "brown": (120, 75, 45),
    "red": (175, 45, 45),
    "orange": (215, 120, 40),
    "yellow": (225, 205, 60),
    "lime": (120, 200, 60),
    "green": (60, 140, 60),
    "cyan": (60, 160, 160),
    "light_blue": (100, 150, 215),
    "blue": (60, 80, 175),
    "purple": (120, 70, 160),
    "magenta": (170, 70, 150),
    "pink": (215, 130, 170),
}


# --- Palette persistence ---
# We ship a built-in palette, but also write a palette.json next to the script on first run.
# Advanced users can tweak palette.json without touching the code.

def ensure_palette_json(palette_path: str) -> None:
    """Write a starter palette.json if one does not already exist.

    The file is optional; the app will run using built-in palettes.
    If present, values in palette.json override the built-in tables.
    """
    try:
        path = Path(palette_path)
        if path.exists():
            return

        bundle = {
            "schema_version": 1,
            "palette": PALETTE,
            "variant_palette": VARIANT_PALETTE,
            "plant_type_palette": PLANT_TYPE_PALETTE,
            "color_palette": COLOR_PALETTE,
            "wood_material_palette": WOOD_MATERIAL_PALETTE,
            "wool_color_palette": WOOL_COLOR_PALETTE,
            "terracotta_color_palette": TERRACOTTA_COLOR_PALETTE,
            "concrete_color_palette": CONCRETE_COLOR_PALETTE,
            "concrete_powder_color_palette": CONCRETE_POWDER_COLOR_PALETTE,
            "carpet_color_palette": CARPET_COLOR_PALETTE,
            "stained_glass_color_palette": STAINED_GLASS_COLOR_PALETTE,
            "stained_glass_pane_color_palette": STAINED_GLASS_PANE_COLOR_PALETTE,
        }
        path.write_text(json.dumps(bundle, indent=2, sort_keys=True), encoding="utf-8")
    except Exception:
        # Non-fatal (read-only folder, bundled app, etc.)
        pass

AIR_LIKE = {"minecraft:air", "minecraft:cave_air", "minecraft:void_air"}
TRANSPARENT_LIKE = {"minecraft:glass", "minecraft:tinted_glass", "minecraft:barrier"}
WATER_LIKE = {"minecraft:water"}


@dataclass
class RenderOptions:
    dimension: str = "minecraft:overworld"
    y_min: int = 0
    y_max: int = 320
    skip_water: bool = False
    hillshade_mode: str = "normal"  # "none", "normal", "strong"

    target_preset: str = "1080p (1920x1080)"
    workers: int = 3
    fast_scan: bool = False
    aggressive_mode: bool = False
    auto_tune: bool = True

    debug_block_samples: bool = False
    debug_log_unknowns: bool = True

    # Output options
    output_name: str = ""
    keep_frames: bool = False

    # Optional render limiting (world block coordinates, inclusive)
    limit_enabled: bool = False
    x_min: int = 0
    z_min: int = 0
    x_max: int = 0
    z_max: int = 0

    # Palette editor override: if use_editor_palette is True, editor_palette
    # is consulted first; blocks missing from it fall back to the default PALETTE.
    use_editor_palette: bool = False
    editor_palette: Optional[Dict[str, Tuple[int, int, int]]] = None


class CancelledError(Exception):
    pass


def clone_render_options(opt: "RenderOptions") -> "RenderOptions":
    """Clone RenderOptions while ignoring runtime-only ad-hoc attributes."""
    field_names = {f.name for f in fields(RenderOptions)}
    data = {k: v for k, v in vars(opt).items() if k in field_names}
    return RenderOptions(**data)




def _app_dir() -> str:
    """Directory to look for external files (palette.json, etc.).

    - When running from source: folder containing this .py
    - When frozen (PyInstaller): folder containing the executable
    """
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


def load_palette_overrides(palette_path: str, log_cb=None, log=None) -> None:
    """Load palette overrides from palette.json and merge into built-in palettes.

    The palette.json written by ensure_palette_json mirrors the internal dict names.
    Users may edit it to tweak colors without editing the .py.

    Expected JSON keys (all optional):
      - palette
      - wood_material_palette
      - plant_type_palette
      - leaves_material_palette
      - carpet_color_palette
      - terracotta_color_palette
      - concrete_color_palette
      - stained_glass_color_palette
      - wool_color_palette

    Values should be RGB arrays like [r,g,b].
    """
    # Back-compat: allow caller to pass log=<callable> instead of log_cb.
    if log is not None and log_cb is None:
        log_cb = log
    def _log(msg: str):
        if log_cb:
            try:
                log_cb(msg)
            except Exception:
                pass

    if not os.path.isfile(palette_path):
        _log(f"Palette overrides: not found at {palette_path} (using built-in palette)")
        return

    try:
        with open(palette_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        _log(f"Palette overrides: failed to read {palette_path}: {type(e).__name__}: {e} (using built-in palette)")
        return

    def _coerce_rgb(v):
        if not (isinstance(v, (list, tuple)) and len(v) == 3):
            raise ValueError("RGB must be [r,g,b]")
        r, g, b = v
        return (int(r), int(g), int(b))

    # Map json keys -> global dict name
    targets = {
        "palette": "PALETTE",
        "wood_material_palette": "WOOD_MATERIAL_PALETTE",
        "plant_type_palette": "PLANT_TYPE_PALETTE",
        "leaves_material_palette": "LEAVES_MATERIAL_PALETTE",
        "carpet_color_palette": "CARPET_COLOR_PALETTE",
        "terracotta_color_palette": "TERRACOTTA_COLOR_PALETTE",
        "concrete_color_palette": "CONCRETE_COLOR_PALETTE",
        "stained_glass_color_palette": "STAINED_GLASS_COLOR_PALETTE",
        "wool_color_palette": "WOOL_COLOR_PALETTE",
    }

    applied = 0
    for json_key, global_name in targets.items():
        if json_key not in data:
            continue
        mapping = data.get(json_key)
        if not isinstance(mapping, dict):
            _log(f"Palette overrides: '{json_key}' is not a JSON object; skipping")
            continue
        g = globals().get(global_name)
        if not isinstance(g, dict):
            _log(f"Palette overrides: internal '{global_name}' missing; skipping")
            continue
        for k, v in mapping.items():
            try:
                g[str(k)] = _coerce_rgb(v)
                applied += 1
            except Exception:
                continue

    # Also handle the palette-editor's rgb_overrides format — flat block_id -> [r,g,b]
    if "rgb_overrides" in data and isinstance(data["rgb_overrides"], dict):
        for k, v in data["rgb_overrides"].items():
            try:
                PALETTE[str(k)] = _coerce_rgb(v)
                applied += 1
            except Exception:
                continue

    # Rebuild fast palette lookup after overrides
    global PALETTE_KEY_TO_IDX, PALETTE_COLOR_TABLE
    PALETTE_KEY_TO_IDX, PALETTE_COLOR_TABLE = build_palette_lookup(PALETTE)
    _log(f"Palette overrides: applied {applied} RGB overrides from {palette_path}")


def apply_palette_overrides(palette_path: str) -> None:
    """Load palette overrides from palette.json. Non-fatal if missing or invalid."""
    # Use a lightweight logger so load issues show up in the console/log panel.
    load_palette_overrides(palette_path, log=lambda m: print(m))

def normalize_log_text(s: str) -> str:
    """Convert literal \\n sequences to real newlines for display/logging."""
    return s.replace("\\r\\n", "\n").replace("\\n", "\n")


def fmt_seconds(seconds: Optional[float]) -> str:
    if seconds is None:
        return ""
    if seconds < 0:
        seconds = 0
    s = int(round(seconds))
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    if h > 0:
        return f"{h}h {m:02d}m {sec:02d}s"
    if m > 0:
        return f"{m}m {sec:02d}s"
    return f"{sec}s"


def safe_filename(name: str) -> str:
    return re.sub(r'[<>:"/\\\\|?*]+', "_", name).strip()


def parse_target_preset(preset: str) -> Optional[Tuple[int, int]]:
    p = preset.strip().lower()
    if "4k" in p:
        return (3840, 2160)
    if "1080" in p:
        return (1920, 1080)
    if "720" in p:
        return (1280, 720)
    if "original" in p or "no limit" in p:
        return None
    if "custom" in p:
        m = re.search(r"(\d+)\s*x\s*(\d+)", preset)
        if m:
            return (int(m.group(1)), int(m.group(2)))
        return None
    m = re.search(r"(\\d+)\\s*x\\s*(\\d+)", preset)
    if m:
        return (int(m.group(1)), int(m.group(2)))
    return (1920, 1080)


def block_key(block) -> str:
    ns = getattr(block, "namespace", None)
    bn = getattr(block, "base_name", None)
    if ns and bn:
        return f"{ns}:{bn}"
    nn = getattr(block, "namespaced_name", None)
    if nn:
        return str(nn)
    return str(block)


def normalize_block_id(block_id: str) -> str:
    bid = str(block_id).strip()

    if bid.startswith("Block(") and bid.endswith(")"):
        bid = bid[len("Block("):-1].strip()

    props = ""
    if "[" in bid and bid.endswith("]"):
        base, props = bid.split("[", 1)
        props = props[:-1]
        bid = base.strip()

    if bid.startswith("universal_minecraft:"):
        bid = "minecraft:" + bid.split(":", 1)[1]

    if bid == "minecraft:plant" and props:
        m = re.search(r'plant_type\\s*=\\s*\\"([^\\"]+)\\"', props)
        if m:
            pt = m.group(1).strip().lower()
            if pt == "grass":
                return "minecraft:grass"
            if pt == "fern":
                return "minecraft:fern"
            if pt in ("tall_grass", "tallgrass"):
                return "minecraft:tall_grass"
            if pt in ("seagrass", "sea_grass"):
                return "minecraft:seagrass"
            return "minecraft:grass"

    if bid == "minecraft:snow_layer":
        return "minecraft:snow"

    return bid


# Aliases for common cross-edition / cross-mapper naming differences.
_ID_ALIASES: Dict[str, str] = {
    # Bedrock / Amulet universal naming → Java texture naming
    "minecraft:grass_path": "minecraft:dirt_path",
    "minecraft:brick_block": "minecraft:bricks",
}


def _canon_block_id_for_palette(raw_id: str) -> str:
    """Convert Amulet/universal block ids to palette keys.

    We keep texture-relevant variants (wood species, carpet color, etc.) and
    strip purely-orientation/state variants (axis, facing, age, etc.).
    """
    # Quick normalization to minecraft namespace (but keep the property string for parsing).
    s = raw_id.replace("universal_minecraft:", "minecraft:")

    # Extract base id and property dict (if any)
    base = s.split("[", 1)[0].split("{", 1)[0]
    props: Dict[str, str] = {}
    if "[" in s and "]" in s:
        inside = s.split("[", 1)[1].split("]", 1)[0]
        for part in inside.split(","):
            part = part.strip()
            if not part or "=" not in part:
                continue
            k, v = part.split("=", 1)
            props[k.strip()] = v.strip().strip('"')

    base = _ID_ALIASES.get(base, base)

    # Wood variants
    material = props.get("material")
    if base == "minecraft:leaves" and material:
        return f"minecraft:{material}_leaves"
    if base == "minecraft:log" and material:
        # Handle nether fungi naming
        if material in ("warped", "crimson"):
            return f"minecraft:{material}_stem"
        return f"minecraft:{material}_log"
    if base == "minecraft:planks" and material:
        return f"minecraft:{material}_planks"
    if base == "minecraft:sapling" and material:
        return f"minecraft:{material}_sapling"
    if base == "minecraft:fence" and material:
        return f"minecraft:{material}_fence"

    # Color variants
    color = props.get("color")
    if base == "minecraft:carpet" and color:
        return f"minecraft:{color}_carpet"
    if base == "minecraft:wool" and color:
        return f"minecraft:{color}_wool"

    # Walls (material-based)
    if base == "minecraft:wall" and material:
        material_map = {
            "deepslate_tile": "deepslate_tile",
            "cobblestone": "cobblestone",
            "mossy_cobblestone": "mossy_cobblestone",
            "brick": "brick",
            "stone_brick": "stone_brick",
        }
        m = material_map.get(material, material)
        return f"minecraft:{m}_wall"

    # Generic cleanup: drop states that rarely affect top-down average color
    return base


def _extract_block_base_and_props(block) -> Tuple[str, Dict[str, str], str]:
    """
    Try to extract a stable base id + properties from an Amulet Block-like object.
    Returns: (base_id_with_minecraft_prefix, props_dict, raw_str)
    """
    raw = _raw_block_id(block)

    # Base id
    base = None
    nn = getattr(block, "namespaced_name", None)
    if nn:
        base = str(nn)

    # Properties
    props: Dict[str, str] = {}
    for attr in ("properties", "states", "state"):
        v = getattr(block, attr, None)
        if isinstance(v, dict):
            for k, val in v.items():
                # Amulet uses NBT tags (StringTag("...") etc). Prefer the underlying value when present.
                vv = getattr(val, "value", None)
                if vv is not None:
                    props[str(k)] = str(vv)
                else:
                    props[str(k)] = str(val)
            break

    # Parse props from string form if needed
    if "[" in raw and raw.endswith("]"):
        try:
            base_s, prop_s = raw.split("[", 1)
            prop_s = prop_s[:-1]
            if base is None:
                base = base_s.strip()
            for m in re.finditer(r'([a-zA-Z0-9_:-]+)\s*=\s*\"([^\"]*)\"', prop_s):
                props.setdefault(m.group(1), m.group(2))
        except Exception:
            pass

    if base is None:
        base = raw.split("[", 1)[0].strip()

    # Convert universal_minecraft:foo -> minecraft:foo
    if base.startswith("universal_minecraft:"):
        base = "minecraft:" + base.split(":", 1)[1]

    return base, props, raw


def classify_block(block, extra_palette: Optional[Dict[str, Tuple[int, int, int]]] = None) -> Tuple[Tuple[int, int, int], str, bool, str]:
    """
    Returns: (rgb, key_used, is_known, reason)

    *extra_palette*, if provided, is checked before the built-in PALETTE.
    Blocks present only in extra_palette are returned as known.
    """
    base, props, raw = _extract_block_base_and_props(block)
    bid = normalize_block_id(raw)
    palette_key = _ID_ALIASES.get(_canon_block_id_for_palette(raw), _canon_block_id_for_palette(raw))

    # Always treat rails as known
    if "rail" in bid:
        return (160, 160, 160), bid, True, "rail"

    wood_type = None
    for k in ("wood_type", "wood", "material", "type", "tree_type"):
        if k in props:
            wood_type = props[k].strip().lower()
            break

    color_name = None
    for k in ("color", "dye_color"):
        if k in props:
            color_name = props[k].strip().lower()
            break

    # Variant-aware wood/leaves coloring
    if wood_type:
        if bid == "minecraft:leaves" or bid.endswith("_leaves"):
            key = f"minecraft:leaves|{wood_type}"
            if key in VARIANT_PALETTE:
                return VARIANT_PALETTE[key], key, True, "variant"
        if bid in ("minecraft:log", "minecraft:planks") or bid.endswith(("_log", "_wood", "_stem", "_hyphae", "_planks")):
            key = f"minecraft:wood|{wood_type}"
            if key in VARIANT_PALETTE:
                return VARIANT_PALETTE[key], key, True, "variant"


    # Variant-aware plant coloring (flowers / foliage)
    if bid in ("minecraft:plant", "minecraft:double_plant", "minecraft:pink_petals", "minecraft:wildflowers") or bid.endswith(("plant", "petals", "wildflowers")):
        plant_type = None
        for k in ("plant_type", "type", "variant"):
            if k in props:
                plant_type = props[k].strip().lower()
                break
        if plant_type and plant_type in PLANT_TYPE_PALETTE:
            return PLANT_TYPE_PALETTE[plant_type], f"{bid}|{plant_type}", True, "variant"

    # Variant-aware color-family blocks (wool, concrete, terracotta, stained glass)
    if color_name and color_name in COLOR_PALETTE:
        if any(x in bid for x in ("wool", "concrete", "terracotta", "stained_glass")):
            return COLOR_PALETTE[color_name], f"{bid}|{color_name}", True, "variant"

    # Direct palette hit (prefer canonical palette key that preserves wood/color variants)
    # Check extra_palette (palette editor) first, then fall back to built-in PALETTE.
    if extra_palette:
        if palette_key in extra_palette:
            return extra_palette[palette_key], palette_key, True, "editor_palette"
        if bid in extra_palette:
            return extra_palette[bid], bid, True, "editor_palette"
    if palette_key in PALETTE:
        return PALETTE[palette_key], palette_key, True, "palette"
    if bid in PALETTE:
        return PALETTE[bid], bid, True, "palette"

    # Generic-ID fallback: resolve IDs like "minecraft:fence" or crop roots
    # using nearby palette family entries before dropping to gray unknown.
    generic_rgb = _generic_palette_family_fallback(bid)
    if generic_rgb is not None:
        return generic_rgb, bid, True, "generic_palette_fallback"

    # Non-gray defaults for big generic buckets
    if bid == "minecraft:leaves":
        return (72, 144, 48), bid, True, "generic"
    if bid == "minecraft:log":
        return (102, 81, 51), bid, True, "generic"
    if bid == "minecraft:planks":
        return (150, 120, 80), bid, True, "generic"
    if bid in ("minecraft:plant", "minecraft:double_plant", "minecraft:leaf_litter", "minecraft:pink_petals", "minecraft:wildflowers"):
        return (96, 170, 72), bid, True, "generic"

    # Heuristics
    if bid.endswith("_leaves"):
        return (72, 144, 48), bid, True, "heuristic"
    if bid.endswith("_log") or bid.endswith("_wood") or bid.endswith("_stem") or bid.endswith("_hyphae"):
        return (102, 81, 51), bid, True, "heuristic"
    if bid.endswith("_planks"):
        return (150, 120, 80), bid, True, "heuristic"
    if bid.endswith("_slab") or bid.endswith("_stairs"):
        return (135, 115, 85), bid, True, "heuristic"
    if "water" in bid or "seagrass" in bid or "kelp" in bid or "bubble_column" in bid:
        return (64, 64, 255), bid, True, "heuristic"
    if "lava" in bid:
        return (255, 80, 0), bid, True, "heuristic"
    if "stone" in bid or "deepslate" in bid or "tuff" in bid:
        return (120, 120, 120), bid, True, "heuristic"
    if "sand" in bid:
        return (220, 210, 150), bid, True, "heuristic"
    if "dirt" in bid or "mud" in bid or "podzol" in bid:
        return (134, 96, 67), bid, True, "heuristic"
    if "gravel" in bid:
        return (150, 150, 150), bid, True, "heuristic"

    return (180, 180, 180), bid, False, "unknown"



def compute_hillshade(height: np.ndarray, mode: str = "normal") -> np.ndarray:
    """Compute hillshade based on slope. Mode controls strength.
    
    Returns array of multipliers [0.5, 1.5] where <1 = shadows, >1 = highlights.
    """
    if mode == "none":
        return np.ones_like(height, dtype=np.float32)
    
    dy, dx = np.gradient(height.astype(np.float32))
    shade = -(dx * -1.0 + dy * -1.0)
    smin, smax = np.percentile(shade, 2), np.percentile(shade, 98)
    if smax - smin < 1e-6:
        return np.ones_like(height, dtype=np.float32)
    shade = (shade - smin) / (smax - smin)
    
    if mode == "strong":
        # Stronger slope effect + curving to make mid-slopes more visible
        shade = np.power(shade, 0.8)  # Brightens mid-tones
        return (0.5 + 0.8 * shade).astype(np.float32)
    else:  # "normal"
        return (0.75 + 0.5 * shade).astype(np.float32)


def compute_altitude_tint(height: np.ndarray, mode: str = "normal") -> np.ndarray:
    """Compute altitude-based color tinting. High = warm, low = cool.
    
    Returns RGB tint overlay as float32 [0.7, 1.3] per channel.
    """
    if mode == "none":
        return np.ones((*height.shape, 3), dtype=np.float32)
    
    h_min, h_max = np.percentile(height, 5), np.percentile(height, 95)
    if h_max - h_min < 1e-6:
        return np.ones((*height.shape, 3), dtype=np.float32)
    
    h_norm = (height - h_min) / (h_max - h_min)  # [0, 1]
    h_norm = np.clip(h_norm, 0, 1)
    
    if mode == "strong":
        strength = 0.12  # 0.88 - 1.12 color range
    else:  # "normal" — no altitude tint, only slope shading
        return np.ones((*height.shape, 3), dtype=np.float32)

    # High altitude: cool (B+, R-), Low altitude: warm (R+, B-)
    tint = np.ones((*height.shape, 3), dtype=np.float32)
    tint[..., 0] = 1.0 - strength * (2 * h_norm - 1)     # Red channel: low=warm
    tint[..., 1] = 1.0                                     # Green: neutral
    tint[..., 2] = 1.0 + strength * (2 * h_norm - 1)     # Blue channel: high=cool
    
    return tint.astype(np.float32)


def _dimension_aliases(dim_id: str) -> List[Any]:
    aliases: List[Any] = [dim_id]
    if ":" in dim_id:
        aliases.append(dim_id.split(":")[-1])
        aliases.append(dim_id.replace("minecraft:", ""))

    low = str(dim_id).lower()
    if "overworld" in low:
        aliases += ["minecraft:overworld", "overworld", "DIM0", 0]
    if "the_nether" in low or low.endswith("nether"):
        aliases += ["minecraft:the_nether", "the_nether", "nether", "DIM-1", -1]
    if "the_end" in low or low.endswith("end"):
        aliases += ["minecraft:the_end", "the_end", "end", "DIM1", 1]

    seen = set()
    out = []
    for a in aliases:
        key = (type(a), a)
        if key in seen:
            continue
        seen.add(key)
        out.append(a)
    return out


def resolve_dimension_id(world: Any, requested: str) -> str:
    dims = getattr(world, "dimensions", None)
    if not dims:
        return requested
    dim_strings = [d for d in dims if isinstance(d, str)]
    for a in _dimension_aliases(requested):
        if isinstance(a, str) and a in dim_strings:
            return a
    return requested


def _score_world_root(root: str, dirs: List[str], files: List[str] = None) -> float:
    dset = set(dirs)
    score = 0.0
    if "region" in dset:
        score += 5.0
    if "DIM-1" in dset or "DIM1" in dset:
        score += 3.0
    if "db" in dset:
        score += 5.0
    return score


def _score_world_root_path(root: str) -> float:
    """Score a world root path by inspecting its immediate entries."""
    try:
        entries = []
        for e in os.listdir(root):
            p = os.path.join(root, e)
            if os.path.isdir(p):
                entries.append(e)
        return _score_world_root(root, entries)
    except Exception:
        return 0.0


def unzip_world_find_roots(zip_path: str, out_dir: str) -> Tuple[str, List[str]]:
    extract_root = os.path.join(out_dir, "extracted")
    os.makedirs(extract_root, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_root)

    candidates: List[Tuple[float, str]] = []
    for root, dirs, files in os.walk(extract_root):
        if "level.dat" in files:
            score = _score_world_root(root, dirs)
            depth = root.count(os.sep) - extract_root.count(os.sep)
            score -= min(depth, 20) * 0.1
            candidates.append((score, root))

    candidates.sort(reverse=True, key=lambda x: x[0])
    return extract_root, [c[1] for c in candidates]


def world_all_chunk_coords(world: Any, dim_id: str) -> List[Tuple[int, int]]:
    if hasattr(world, "all_chunk_coords"):
        try:
            coords = list(world.all_chunk_coords(dim_id))
        except TypeError:
            coords = list(world.all_chunk_coords())
        return coords
    if hasattr(world, "get_dimension"):
        dim = world.get_dimension(dim_id)
        if hasattr(dim, "all_chunk_coords"):
            return list(dim.all_chunk_coords())
    raise AttributeError("Could not enumerate chunks (no supported all_chunk_coords API found).")


def _is_chunk_missing_exc(e: Exception) -> bool:
    return e.__class__.__name__ == "ChunkDoesNotExist"


def probe_chunk_getter(world: Any, dim_id: str, coords: List[Tuple[int, int]]) -> Callable[[int, int], Any]:
    if not hasattr(world, "get_chunk"):
        if hasattr(world, "get_dimension"):
            dim = world.get_dimension(dim_id)
            if hasattr(dim, "get_chunk"):
                return lambda cx, cz: dim.get_chunk(cx, cz)
        raise AttributeError("World has no get_chunk and no dimension.get_chunk fallback.")

    patterns: List[Callable[[int, int], Any]] = [
        lambda cx, cz: world.get_chunk(cx, cz, dim_id),
        lambda cx, cz: world.get_chunk(dim_id, cx, cz),
        lambda cx, cz: world.get_chunk(cx, cz, dimension=dim_id),
        lambda cx, cz: world.get_chunk(cx, cz),
    ]

    probe_n = min(50, len(coords))
    for fn in patterns:
        for i in range(probe_n):
            cx, cz = coords[i]
            try:
                ch = fn(cx, cz)
                if ch is not None:
                    return fn
            except Exception as e:
                if _is_chunk_missing_exc(e):
                    continue
                if isinstance(e, TypeError):
                    break
                break

    dims = getattr(world, "dimensions", None)
    raise RuntimeError(
        "Could not determine a working get_chunk(...) signature.\\n"
        f"World type: {type(world)}\\n"
        f"Dimension requested/resolved: {dim_id}\\n"
        f"world.dimensions: {dims}\\n"
    )


def probe_world_block_getter(world: Any, dim_id: str, sample_coord: Tuple[int, int], y_probe: int) -> Optional[Callable[[int, int, int], Any]]:
    """Best-effort probe for world-level block access API differences across Amulet versions."""
    if not hasattr(world, "get_block"):
        return None

    sx, sz = sample_coord
    patterns: List[Callable[[int, int, int], Any]] = [
        lambda x, y, z: world.get_block(x, y, z, dim_id),
        lambda x, y, z: world.get_block(dim_id, x, y, z),
        lambda x, y, z: world.get_block(x, y, z, dimension=dim_id),
        lambda x, y, z: world.get_block(x, y, z),
    ]

    for fn in patterns:
        try:
            _ = fn(sx, y_probe, sz)
            return fn
        except Exception as e:
            if _is_chunk_missing_exc(e):
                return fn
            continue

    return None


def write_debug_snapshot(
    debug_path: str,
    header: str,
    samples_raw: List[str],
    colored_cols: int,
    air_only_cols: int,
    unknown_cols: int,
    unknown_norm_counts: Counter,
    unknown_raw_counts: Optional[Counter] = None,
    exact_key_miss_counts: Optional[Counter] = None,
    base_id_miss_counts: Optional[Counter] = None,
):
    os.makedirs(os.path.dirname(debug_path), exist_ok=True)

    # Normalize samples for quick review
    samples_norm = sorted({normalize_block_id(s) for s in samples_raw})

    lines: List[str] = []
    lines.append(header)
    lines.append("")
    lines.append("Raw block samples (first-seen unique):")
    for s in samples_raw:
        lines.append(f"  {s}")
    lines.append("")
    lines.append("Normalized block samples:")
    for s in samples_norm:
        lines.append(f"  {s}")
    lines.append("")
    lines.append("Column scan counts:")
    lines.append(f"  colored={colored_cols}")
    lines.append(f"  air_only={air_only_cols}")
    lines.append(f"  unknown_colored={unknown_cols}")
    lines.append("")
    ek = exact_key_miss_counts or Counter()
    bm = base_id_miss_counts or Counter()
    lines.append("Palette coverage breakdown (unknowns):")
    lines.append("  exact_key_misses: base ID exists in palette, but variant/property key missing")
    lines.append("  base_id_misses: base ID not present in palette at all")
    lines.append("")
    lines.append("Top exact-key misses (base IDs):")
    if ek:
        for bid, cnt in ek.most_common(30):
            lines.append(f"  {cnt:6d}  {bid}")
    else:
        lines.append("  (none)")
    lines.append("")
    lines.append("Top base-ID misses (base IDs):")
    if bm:
        for bid, cnt in bm.most_common(30):
            lines.append(f"  {cnt:6d}  {bid}")
    else:
        lines.append("  (none)")
    lines.append("")
    lines.append("Top unknown normalized block IDs (these are rendered gray):")
    for bid, cnt in unknown_norm_counts.most_common(50):
        lines.append(f"  {cnt:8d}  {bid}")

    if unknown_raw_counts:
        lines.append("")
        lines.append("Top unknown RAW block IDs (most useful for diagnosing variants):")
        for bid, cnt in unknown_raw_counts.most_common(75):
            # Keep line lengths reasonable
            s = str(bid)
            if len(s) > 160:
                s = s[:157] + "..."
            lines.append(f"  {cnt:8d}  {s}")

    # Ensure real newlines (avoid literal "\n" sequences)
    with open(debug_path, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(lines))



def _write_frame_unknowns_json(json_path: str, unknown_norm_counts: Counter) -> None:
    """Write a machine-readable JSON file listing unknown block IDs from a single frame render.

    The file is placed alongside the frame PNG (same basename with _unknowns.json suffix) and
    is later aggregated across all frames by worker_run into a run-level unknown_blocks.json.
    """
    try:
        os.makedirs(os.path.dirname(json_path) or ".", exist_ok=True)
        payload = {k: v for k, v in unknown_norm_counts.most_common()}
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
    except Exception:
        pass  # Non-fatal: debug aid only


def compute_blocks_per_pixel(width_blocks: int, height_blocks: int, target: Optional[Tuple[int, int]]) -> int:
    if target is None:
        return 1
    max_w, max_h = target
    if max_w <= 0 or max_h <= 0:
        return 1
    bpp_w = int(np.ceil(width_blocks / max_w)) if width_blocks > max_w else 1
    bpp_h = int(np.ceil(height_blocks / max_h)) if height_blocks > max_h else 1
    return max(1, bpp_w, bpp_h)


def fit_to_target(img: Image.Image, target: Optional[Tuple[int, int]]) -> Image.Image:
    if target is None:
        return img
    max_w, max_h = target
    w, h = img.size
    if w == 0 or h == 0:
        return img
    scale = min(max_w / w, max_h / h)
    new_w = max(1, int(round(w * scale)))
    new_h = max(1, int(round(h * scale)))
    if (new_w, new_h) == (w, h):
        return img
    return img.resize((new_w, new_h), resample=Image.NEAREST)


def find_top_block_in_column(
    chunk: Any,
    lx: int,
    lz: int,
    y_min: int,
    y_max: int,
    skip_water: bool,
    fast_scan: bool,
    cancel_event: Any = None,
    block_getter: Optional[Callable[[int, int, int], Any]] = None,
) -> Tuple[bool, int, str]:
    get_block = block_getter if callable(block_getter) else getattr(chunk, "get_block", None)
    if not callable(get_block):
        raise TypeError(f"Chunk block accessor is not callable: {type(chunk)}")

    if not fast_scan:
        for y in range(y_max, y_min - 1, -1):
            b = get_block(lx, y, lz)
            raw = _raw_block_id(b)
            bid = normalize_block_id(raw)
            if bid in AIR_LIKE or bid in TRANSPARENT_LIKE:
                continue
            if skip_water and bid in WATER_LIKE:
                continue
            return True, y, raw
        return False, y_min, "minecraft:air"
    step = 8

    # Fast scan goal (default): catch *roofs* and the true surface while avoiding caves.
    #
    # 1) Coarse scan downward in steps of 8.
    # 2) When we hit a non-skippable (solid) block at y_hit, jump UP by 20 blocks
    #    (user-requested safety margin) and then scan DOWN 1-by-1 until we find the
    #    highest non-skippable block in that column.
    #
    # This is intentionally biased to find thin roofs: if the coarse scan hits a floor
    # inside a building, the +20 "rewind" is meant to recover the roof above.

    def is_skippable(bid: str) -> bool:
        if bid in AIR_LIKE or bid in TRANSPARENT_LIKE:
            return True
        if skip_water and bid in WATER_LIKE:
            return True
        return False

    y_hit: Optional[int] = None
    y = y_max
    while y >= y_min:
        if cancel_event is not None and cancel_event.is_set():
            return False, 0, ""
        b = get_block(lx, y, lz)
        raw = _raw_block_id(b)
        bid = normalize_block_id(raw)
        if not is_skippable(bid):
            y_hit = y
            break
        y -= step

    if y_hit is None:
        return False, y_min, "minecraft:air"

    refine_top = min(y_max, y_hit + max(20, step * 3))
    for yy in range(refine_top, y_min - 1, -1):
        if cancel_event is not None and cancel_event.is_set():
            return False, 0, ""
        b = get_block(lx, yy, lz)
        raw = _raw_block_id(b)
        bid = normalize_block_id(raw)
        if is_skippable(bid):
            continue
        return True, yy, raw

    return False, y_min, "minecraft:air"


def find_snapshot_sources(folder: str, dimension: str = "") -> List[SnapshotInput]:
    # Always discover all three types (caches, ZIPs, world folders).
    # Cache mode only controls whether *new* caches are built — it never
    # suppresses reading of existing .wmtt4mc files.
    inputs = discover_snapshot_inputs(folder, dimension=dimension)

    def sort_key(item: SnapshotInput):
        idx = extract_index_from_name(item.display_name)
        if idx >= 0:
            return (0, idx, item.display_name.lower())
        return (1, 0, item.display_name.lower())

    return list(reversed(sorted(inputs, key=sort_key)))


def snapshot_input_from_path(path: str) -> SnapshotInput:
    display_name = os.path.basename(path.rstrip("\\/"))
    if is_cache_file(path):
        return SnapshotInput(kind="cache", path=path, display_name=display_name, sort_name=display_name, cache_path=path)
    if is_world_folder(path):
        return SnapshotInput(kind="folder", path=path, display_name=display_name, sort_name=display_name)
    return SnapshotInput(kind="zip", path=path, display_name=display_name, sort_name=display_name)


_REGION_PATH_RE = re.compile(r"(?:^|/)region/r\.(-?\d+)\.(-?\d+)\.mca$", re.IGNORECASE)


def _path_matches_dimension(path_lower: str, dimension: str) -> bool:
    dim = str(dimension or "minecraft:overworld").strip().lower()
    # Dimension folders are Java conventions; for overworld, exclude nether/end subfolders.
    if dim == "minecraft:the_nether":
        return "/dim-1/region/" in path_lower
    if dim == "minecraft:the_end":
        return "/dim1/region/" in path_lower
    return ("/region/" in path_lower) and ("/dim-1/" not in path_lower) and ("/dim1/" not in path_lower)


def _estimate_block_area_from_region_paths(paths: List[str], dimension: str) -> Optional[int]:
    min_rx = min_rz = None
    max_rx = max_rz = None
    for raw_path in paths:
        p = str(raw_path).replace("\\", "/")
        low = p.lower()
        if not _path_matches_dimension(low, dimension):
            continue
        m = _REGION_PATH_RE.search(low)
        if not m:
            continue
        rx = int(m.group(1))
        rz = int(m.group(2))
        min_rx = rx if (min_rx is None) else min(min_rx, rx)
        max_rx = rx if (max_rx is None) else max(max_rx, rx)
        min_rz = rz if (min_rz is None) else min(min_rz, rz)
        max_rz = rz if (max_rz is None) else max(max_rz, rz)

    if None in (min_rx, max_rx, min_rz, max_rz):
        return None

    # A region is 32x32 chunks; a chunk is 16x16 blocks.
    chunks_x = (int(max_rx) - int(min_rx) + 1) * 32
    chunks_z = (int(max_rz) - int(min_rz) + 1) * 32
    blocks_x = chunks_x * 16
    blocks_z = chunks_z * 16
    return max(1, int(blocks_x) * int(blocks_z))


def _estimate_bedrock_block_area_from_entries(paths: List[str], size_lookup: Optional[Dict[str, int]] = None) -> Optional[int]:
    db_bytes = 0
    db_files = 0
    for raw_path in paths:
        p = str(raw_path).replace("\\", "/")
        low = p.lower()
        if "/db/" not in low:
            continue
        leaf = low.rsplit("/", 1)[-1]
        if leaf in {"current", "lock", "manifest"}:
            continue
        if not (
            leaf.endswith(".ldb")
            or leaf.endswith(".sst")
            or leaf.endswith(".log")
            or leaf.startswith("manifest-")
        ):
            continue
        db_files += 1
        if size_lookup is not None:
            db_bytes += int(size_lookup.get(raw_path, 0) or 0)

    if db_files <= 0:
        return None

    # Heuristic: ~8 KiB of DB payload per active chunk entry.
    if db_bytes <= 0:
        est_chunks = max(64, db_files * 64)
    else:
        est_chunks = max(64, int(db_bytes // 8192))
    return max(1, int(est_chunks) * 256)


def estimate_snapshot_block_area_with_source(snapshot: SnapshotInput, dimension: str) -> Tuple[Optional[int], str]:
    """Best-effort, fast block-area estimate for progress/ETA weighting.

    Order of preference:
    1) .wmtt4mc cache header bounds (exact cached extent)
    2) Java region filenames (ZIP/folder metadata scan, no world load)
    3) Bedrock LevelDB footprint heuristic (ZIP/folder metadata scan)
    """
    try:
        path = snapshot.path
        if is_cache_file(path) and os.path.isfile(path):
            h = read_cache_header(path)
            min_cx = int(h.get("min_cx"))
            max_cx = int(h.get("max_cx"))
            min_cz = int(h.get("min_cz"))
            max_cz = int(h.get("max_cz"))
            chunks_x = max(0, max_cx - min_cx + 1)
            chunks_z = max(0, max_cz - min_cz + 1)
            if chunks_x > 0 and chunks_z > 0:
                return int(chunks_x * 16) * int(chunks_z * 16), "cache-header"
    except Exception:
        pass

    src = snapshot.raw_path or snapshot.path
    try:
        if os.path.isfile(src) and src.lower().endswith(".zip") and zipfile.is_zipfile(src):
            with zipfile.ZipFile(src, "r") as zf:
                names = zf.namelist()
                area = _estimate_block_area_from_region_paths(names, dimension)
                if area is not None:
                    return area, "java-region"
                try:
                    size_lookup = {zi.filename: int(getattr(zi, "file_size", 0) or 0) for zi in zf.infolist()}
                except Exception:
                    size_lookup = None
                area = _estimate_bedrock_block_area_from_entries(names, size_lookup=size_lookup)
                if area is not None:
                    return area, "bedrock-db-size"
                return None, "fallback"
        if os.path.isdir(src):
            region_candidates: List[str] = []
            bedrock_candidates: List[str] = []
            bedrock_sizes: Dict[str, int] = {}
            for root, _dirs, files in os.walk(src):
                for fn in files:
                    full_path = os.path.join(root, fn)
                    if fn.lower().endswith(".mca"):
                        rel = os.path.relpath(full_path, src)
                        region_candidates.append(rel)
                    rel_any = os.path.relpath(full_path, src)
                    bedrock_candidates.append(rel_any)
                    try:
                        bedrock_sizes[rel_any] = int(os.path.getsize(full_path))
                    except Exception:
                        pass
            area = _estimate_block_area_from_region_paths(region_candidates, dimension)
            if area is not None:
                return area, "java-region"
            area = _estimate_bedrock_block_area_from_entries(bedrock_candidates, size_lookup=bedrock_sizes)
            if area is not None:
                return area, "bedrock-db-size"
    except Exception:
        pass

    return None, "fallback"


def estimate_snapshot_block_area(snapshot: SnapshotInput, dimension: str) -> Optional[int]:
    area, _source = estimate_snapshot_block_area_with_source(snapshot, dimension)
    return area


def _resolve_snapshot_world_roots(source_path: str, out_dir: str) -> Tuple[Optional[str], List[str]]:
    if is_world_folder(source_path):
        return None, [source_path]
    if os.path.isfile(source_path) and source_path.lower().endswith(".zip"):
        extract_root, candidates = unzip_world_find_roots(source_path, out_dir)
        return extract_root, candidates
    raise RuntimeError(f"Unsupported snapshot source: {source_path}")


def _scan_column_surface_and_segments(
    get_block: Callable[[int, int, int], Any],
    lx: int,
    lz: int,
    y_min: int,
    y_max: int,
    include_segments: bool,
) -> Tuple[Tuple[bool, int, str], Tuple[bool, int, str], List[Tuple[int, int, str]]]:
    top_found = False
    top_y = y_min
    top_raw = "minecraft:air"
    dry_found = False
    dry_y = y_min
    dry_raw = "minecraft:air"

    segments: List[Tuple[int, int, str]] = []
    current_raw = None
    current_top = y_max
    current_bottom = y_max

    for y in range(y_max, y_min - 1, -1):
        block = get_block(lx, y, lz)
        raw = _raw_block_id(block)
        norm = normalize_block_id(raw)

        if not top_found and norm not in AIR_LIKE and norm not in TRANSPARENT_LIKE:
            top_found = True
            top_y = y
            top_raw = raw
        if not dry_found and norm not in AIR_LIKE and norm not in TRANSPARENT_LIKE and norm not in WATER_LIKE:
            dry_found = True
            dry_y = y
            dry_raw = raw

        if include_segments:
            if current_raw is None:
                current_raw = raw
                current_top = y
                current_bottom = y
            elif raw == current_raw:
                current_bottom = y
            else:
                segments.append((current_top, current_bottom, current_raw))
                current_raw = raw
                current_top = y
                current_bottom = y

    if include_segments and current_raw is not None:
        segments.append((current_top, current_bottom, current_raw))

    return (top_found, top_y, top_raw), (dry_found, dry_y, dry_raw), segments


def _scan_chunk_cache_arrays(chunk: Any, y_min: int, y_max: int, include_segments: bool) -> Optional[Dict[str, Any]]:
    """Scan one chunk and return raw block arrays/segments for cache encoding."""
    chunk_get_block = getattr(chunk, "get_block", None)
    if not callable(chunk_get_block):
        return None

    top_found = np.zeros((16, 16), dtype=np.uint8)
    top_y_arr = np.full((16, 16), int(y_min), dtype=np.int16)
    dry_found = np.zeros((16, 16), dtype=np.uint8)
    dry_y_arr = np.full((16, 16), int(y_min), dtype=np.int16)

    top_raw: List[str] = ["minecraft:air"] * 256
    dry_raw: List[str] = ["minecraft:air"] * 256

    offsets = np.zeros(257, dtype=np.uint32)
    seg_top: List[int] = []
    seg_bottom: List[int] = []
    seg_raw: List[str] = []

    col_index = 0
    for lz in range(16):
        for lx in range(16):
            top_info, dry_info, segments = _scan_column_surface_and_segments(
                chunk_get_block,
                lx,
                lz,
                int(y_min),
                int(y_max),
                include_segments,
            )
            flat = lz * 16 + lx
            if top_info[0]:
                top_found[lz, lx] = 1
                top_y_arr[lz, lx] = int(top_info[1])
                top_raw[flat] = str(top_info[2])
            if dry_info[0]:
                dry_found[lz, lx] = 1
                dry_y_arr[lz, lx] = int(dry_info[1])
                dry_raw[flat] = str(dry_info[2])

            if include_segments:
                offsets[col_index] = len(seg_top)
                for seg_hi, seg_lo, seg_block in segments:
                    seg_top.append(int(seg_hi))
                    seg_bottom.append(int(seg_lo))
                    seg_raw.append(str(seg_block))
                col_index += 1

    if include_segments:
        offsets[256] = len(seg_top)

    return {
        "top_found": top_found,
        "top_y": top_y_arr,
        "top_raw": top_raw,
        "dry_found": dry_found,
        "dry_y": dry_y_arr,
        "dry_raw": dry_raw,
        "offsets": offsets,
        "seg_top": seg_top,
        "seg_bottom": seg_bottom,
        "seg_raw": seg_raw,
    }


def _safe_cleanup_dir(path: Optional[str], retries: int = 8, delay_sec: float = 0.25) -> None:
    if not path or (not os.path.isdir(path)):
        return
    last_exc: Optional[Exception] = None
    for _ in range(max(1, retries)):
        try:
            shutil.rmtree(path)
            return
        except Exception as e:
            last_exc = e
            time.sleep(delay_sec)
    if last_exc is not None:
        raise last_exc


# --- Per-process state for the cache worker pool ---
# Each worker process initialises these once via _cache_worker_init().
_cache_worker_world = None
_cache_worker_get_chunk = None
_cache_worker_y_min: int = 0
_cache_worker_y_max: int = 320
_cache_worker_include_segments: bool = False


def _cache_worker_init(
    world_root: str,
    dim_id: str,
    y_min: int,
    y_max: int,
    include_segments: bool,
    probe_coord: Tuple[int, int],
) -> None:
    """Pool initializer: open the amulet world once per worker process and keep it alive."""
    import sys, io
    # Suppress amulet/library loading messages that would flood the terminal.
    # stderr is kept open so genuine errors can still be seen.
    sys.stdout = io.StringIO()

    global _cache_worker_world, _cache_worker_get_chunk
    global _cache_worker_y_min, _cache_worker_y_max, _cache_worker_include_segments
    _cache_worker_y_min = y_min
    _cache_worker_y_max = y_max
    _cache_worker_include_segments = include_segments
    _cache_worker_world = amulet.load_level(world_root)
    _cache_worker_get_chunk = probe_chunk_getter(_cache_worker_world, dim_id, [probe_coord])


def _cache_scan_one_chunk(coord: Tuple[int, int]) -> Tuple[int, int, Optional[Dict[str, Any]]]:
    """Pool task: scan a single chunk using the already-open world in this process."""
    cx, cz = coord
    try:
        chunk = _cache_worker_get_chunk(cx, cz)
    except Exception:
        return cx, cz, None
    if chunk is None:
        return cx, cz, None
    return cx, cz, _scan_chunk_cache_arrays(
        chunk,
        _cache_worker_y_min,
        _cache_worker_y_max,
        _cache_worker_include_segments,
    )


def build_snapshot_cache(
    snapshot: SnapshotInput,
    cache_mode: str,
    dimension: str,
    y_min: int,
    y_max: int,
    log_cb: Optional[Callable[[str], None]] = None,
    progress_cb: Optional[Callable[[int, int, float], None]] = None,
    cancel_event: Optional[threading.Event] = None,
) -> str:
    source_path = snapshot.raw_path or snapshot.path
    if is_cache_file(source_path):
        raise RuntimeError("Cannot build a cache from an existing .wmtt4mc cache file.")

    cache_path = sidecar_cache_path(source_path, dimension, cache_mode)
    world0 = None
    tmpdir = tempfile.mkdtemp(prefix="wmtt4mc_cache_")

    try:
        t0_total = time.time()
        _tmp_root, candidates = _resolve_snapshot_world_roots(source_path, tmpdir)
        if not candidates:
            raise RuntimeError("Could not find a world folder to cache (no level.dat found).")

        world_root = sorted(
            [(r, _score_world_root_path(r)) for r in candidates],
            key=lambda t: t[1], reverse=True
        )[0][0]
        if amulet is None or not hasattr(amulet, "load_level"):
            raise RuntimeError("Amulet API unavailable. Cannot build cache.")

        # Open world once in the main process just to enumerate chunks.
        t0_load = time.time()
        world0 = amulet.load_level(world_root)
        dim_id = resolve_dimension_id(world0, dimension)
        chunk_coords = world_all_chunk_coords(world0, dim_id)
        if not chunk_coords:
            raise RuntimeError("No chunks found in the requested dimension.")
        chunk_coords = sorted(chunk_coords, key=lambda t: (t[1], t[0]))
        world0.close()
        world0 = None
        t_load = time.time() - t0_load

        min_cx = min(cx for cx, _ in chunk_coords)
        max_cx = max(cx for cx, _ in chunk_coords)
        min_cz = min(cz for _, cz in chunk_coords)
        max_cz = max(cz for _, cz in chunk_coords)

        signature = build_source_signature(source_path)
        metadata = {
            **signature,
            "cache_mode": cache_mode,
            "dimension": dim_id,
            "y_min": int(y_min),
            "y_max": int(y_max),
            "min_cx": int(min_cx),
            "max_cx": int(max_cx),
            "min_cz": int(min_cz),
            "max_cz": int(max_cz),
        }
        writer = CacheWriter(cache_path, metadata)

        include_segments = cache_mode == CACHE_MODE_ALL_BLOCKS
        total = len(chunk_coords)

        # Use a single worker process so only one amulet world handle is open at
        # a time.  Multiple concurrent opens conflict on LevelDB (Bedrock) and
        # may also fail on Java worlds.  One worker still lets scanning run in a
        # separate process (no GIL contention with the SQLite-writing main thread)
        # and is strictly better than zero extra processes.
        n_workers = 1

        if log_cb is not None:
            log_cb(
                f"Scanning {total} chunks for {snapshot.display_name} "
                f"(starting scanning worker — first launch may take 20-60 s)"
            )

        processed = 0
        written_chunks = 0

        # Use 'spawn' context explicitly for Windows compatibility.
        ctx = multiprocessing.get_context("spawn")
        try:
            pool = ctx.Pool(
                processes=n_workers,
                initializer=_cache_worker_init,
                initargs=(world_root, dim_id, int(y_min), int(y_max), include_segments, chunk_coords[0]),
            )
        except Exception as pool_exc:
            raise RuntimeError(
                f"Failed to start cache scanning worker: {pool_exc}"
            ) from pool_exc

        if log_cb is not None:
            log_cb(f"Worker ready. Scanning {total} chunks for {snapshot.display_name}...")

        # A watcher thread terminates the pool within 0.3 s of cancel being set,
        # which unblocks the for-loop below without needing the non-standard
        # IMapIterator.next(timeout=) call (unavailable on plain generators).
        _stop_watcher = threading.Event()

        def _cancel_watcher_fn(_evt=_stop_watcher, _ce=cancel_event, _p=pool):
            while not _evt.wait(timeout=0.3):
                if _ce is not None and _ce.is_set():
                    try:
                        _p.terminate()
                    except Exception:
                        pass
                    return

        _watcher = threading.Thread(target=_cancel_watcher_fn, daemon=True)
        _watcher.start()

        try:
            for cx, cz, scanned in pool.imap_unordered(
                _cache_scan_one_chunk, chunk_coords, chunksize=16
            ):
                if cancel_event is not None and cancel_event.is_set():
                    pool.terminate()
                    raise CancelledError("Cancelled during cache build.")

                if scanned is not None:
                    top_id = np.zeros((16, 16), dtype=np.uint32)
                    dry_id = np.zeros((16, 16), dtype=np.uint32)
                    for lz in range(16):
                        for lx in range(16):
                            flat = lz * 16 + lx
                            if int(scanned["top_found"][lz, lx]) != 0:
                                top_id[lz, lx] = writer.ensure_block_id(scanned["top_raw"][flat])
                            if int(scanned["dry_found"][lz, lx]) != 0:
                                dry_id[lz, lx] = writer.ensure_block_id(scanned["dry_raw"][flat])

                    if include_segments:
                        seg_block_id = np.asarray(
                            [writer.ensure_block_id(raw) for raw in scanned["seg_raw"]],
                            dtype=np.uint32,
                        )
                        deep_payload = encode_deep_payload(
                            scanned["offsets"],
                            np.asarray(scanned["seg_top"], dtype=np.int16),
                            np.asarray(scanned["seg_bottom"], dtype=np.int16),
                            seg_block_id,
                        )
                    else:
                        deep_payload = None

                    surface_payload = encode_surface_payload(
                        top_id,
                        scanned["top_y"],
                        scanned["top_found"],
                        dry_id,
                        scanned["dry_y"],
                        scanned["dry_found"],
                    )
                    writer.write_chunk(cx, cz, surface_payload, deep_payload)
                    written_chunks += 1

                processed += 1
                if progress_cb is not None:
                    progress_cb(processed, total, processed / max(1, total))
                if log_cb is not None and (processed == 1 or (processed % 200) == 0 or processed == total):
                    log_cb(f"Caching {snapshot.display_name}: chunk {processed}/{total}")

            pool.close()
            pool.join()
        except CancelledError:
            pool.terminate()
            _jt = threading.Thread(target=pool.join, daemon=True)
            _jt.start()
            _jt.join(timeout=5.0)
            raise
        except Exception:
            pool.terminate()
            _jt = threading.Thread(target=pool.join, daemon=True)
            _jt.start()
            _jt.join(timeout=5.0)
            if cancel_event is not None and cancel_event.is_set():
                raise CancelledError("Cancelled during cache build.")
            raise
        finally:
            _stop_watcher.set()
            _watcher.join(timeout=1.0)

        writer.finalize()
        t_total = time.time() - t0_total
        if log_cb is not None:
            log_cb(
                f"Cache stats for {snapshot.display_name}: "
                f"setup+load={t_load:.1f}s | workers={n_workers} | "
                f"chunks_written={written_chunks}/{total} | "
                f"avg_rate={(written_chunks / max(0.001, t_total)):.1f} chunks/s | "
                f"total={t_total:.1f}s"
            )
            log_cb(f"Cache written: {cache_path}")
        return cache_path
    finally:
        try:
            if world0 is not None:
                world0.close()
        except Exception:
            pass
        try:
            _safe_cleanup_dir(tmpdir)
        except Exception:
            pass


def _project_deep_chunk_arrays(
    deep_payload: bytes,
    block_lookup: List[str],
    y_min: int,
    y_max: int,
) -> Dict[str, np.ndarray]:
    deep = decode_deep_payload(deep_payload)
    offsets = deep["offsets"]
    seg_top = deep["seg_top"]
    seg_bottom = deep["seg_bottom"]
    seg_block_id = deep["seg_block_id"]

    top_id = np.zeros((16, 16), dtype=np.uint32)
    top_y_arr = np.full((16, 16), int(y_min), dtype=np.int16)
    top_found = np.zeros((16, 16), dtype=np.uint8)
    dry_id = np.zeros((16, 16), dtype=np.uint32)
    dry_y_arr = np.full((16, 16), int(y_min), dtype=np.int16)
    dry_found = np.zeros((16, 16), dtype=np.uint8)

    for col in range(256):
        start = int(offsets[col])
        end = int(offsets[col + 1])
        lz = col // 16
        lx = col % 16
        for idx in range(start, end):
            seg_hi = int(seg_top[idx])
            seg_lo = int(seg_bottom[idx])
            if seg_lo > y_max or seg_hi < y_min:
                continue
            block_id = int(seg_block_id[idx])
            raw = block_lookup[block_id] if 0 <= block_id < len(block_lookup) else "minecraft:air"
            norm = normalize_block_id(raw)
            visible_y = min(seg_hi, y_max)
            if (not top_found[lz, lx]) and norm not in AIR_LIKE and norm not in TRANSPARENT_LIKE:
                top_found[lz, lx] = 1
                top_y_arr[lz, lx] = visible_y
                top_id[lz, lx] = block_id
            if (not dry_found[lz, lx]) and norm not in AIR_LIKE and norm not in TRANSPARENT_LIKE and norm not in WATER_LIKE:
                dry_found[lz, lx] = 1
                dry_y_arr[lz, lx] = visible_y
                dry_id[lz, lx] = block_id
            if top_found[lz, lx] and dry_found[lz, lx]:
                break

    return {
        "top_id": top_id,
        "top_y": top_y_arr,
        "top_found": top_found,
        "dry_id": dry_id,
        "dry_y": dry_y_arr,
        "dry_found": dry_found,
    }


def render_cached_world_map(
    cache_path: str,
    out_png: str,
    opt: RenderOptions,
    log_cb: Optional[Callable[[str], None]] = None,
    progress_cb: Optional[Callable[[int, int, float], None]] = None,
    cancel_event: Optional[threading.Event] = None,
) -> tuple:
    header = read_cache_header(cache_path)
    cache_mode = str(header.get("cache_mode", CACHE_MODE_SURFACE))
    cache_dimension = str(header.get("dimension", opt.dimension))
    cached_y_min = int(header.get("y_min", opt.y_min))
    cached_y_max = int(header.get("y_max", opt.y_max))

    if cache_dimension != opt.dimension:
        raise RuntimeError(f"Cache dimension mismatch: cache={cache_dimension}, requested={opt.dimension}")
    if cache_mode == CACHE_MODE_SURFACE and (opt.y_min != cached_y_min or opt.y_max != cached_y_max):
        raise RuntimeError(
            f"Surface cache only supports Y=[{cached_y_min},{cached_y_max}] (its original build range). "
            f"You requested Y=[{opt.y_min},{opt.y_max}]. "
            "Change your Y range to match, or rebuild the cache using 'Cache all blocks'."
        )
    if cache_mode == CACHE_MODE_ALL_BLOCKS and (opt.y_min < cached_y_min or opt.y_max > cached_y_max):
        raise RuntimeError(
            f"Requested Y=[{opt.y_min},{opt.y_max}] is outside the all-blocks cache range [{cached_y_min},{cached_y_max}]. "
            "Rebuild the cache with a wider Y range to cover your selection."
        )

    min_cx_filter = max_cx_filter = min_cz_filter = max_cz_filter = None
    if opt.limit_enabled:
        x1 = int(min(opt.x_min, opt.x_max))
        x2 = int(max(opt.x_min, opt.x_max))
        z1 = int(min(opt.z_min, opt.z_max))
        z2 = int(max(opt.z_min, opt.z_max))
        min_cx_filter = x1 // 16 if x1 >= 0 else -((-x1 - 1) // 16) - 1
        max_cx_filter = x2 // 16 if x2 >= 0 else -((-x2 - 1) // 16) - 1
        min_cz_filter = z1 // 16 if z1 >= 0 else -((-z1 - 1) // 16) - 1
        max_cz_filter = z2 // 16 if z2 >= 0 else -((-z2 - 1) // 16) - 1

    rows = list(iter_chunk_rows(cache_path, min_cx_filter, max_cx_filter, min_cz_filter, max_cz_filter))
    if not rows:
        raise RuntimeError("No cached chunks found in the requested area.")

    chunk_coords = [(cx, cz) for cx, cz, _surface_payload, _deep_payload in rows]
    min_cx = min(cx for cx, _ in chunk_coords)
    max_cx = max(cx for cx, _ in chunk_coords)
    min_cz = min(cz for _, cz in chunk_coords)
    max_cz = max(cz for _, cz in chunk_coords)

    min_x = min_cx * 16
    max_x = (max_cx + 1) * 16 - 1
    min_z = min_cz * 16
    max_z = (max_cz + 1) * 16 - 1

    width_blocks = max_x - min_x + 1
    height_blocks = max_z - min_z + 1
    target = parse_target_preset(opt.target_preset)
    bpp = compute_blocks_per_pixel(width_blocks, height_blocks, target)
    w_px = int(np.ceil(width_blocks / bpp))
    h_px = int(np.ceil(height_blocks / bpp))

    rgb = np.zeros((h_px, w_px, 3), dtype=np.uint8)
    hmap = np.full((h_px, w_px), opt.y_min, dtype=np.int16)
    block_lookup = read_block_lookup(cache_path)
    colored_cols = 0
    air_only_cols = 0
    chunks_rendered = 0
    chunks_total = len(rows)
    unknown_norm_counts: Counter = Counter()

    def iter_sample_positions(base: int, size: int, min_axis: int) -> List[int]:
        offset = (min_axis - base) % bpp
        start = base + offset
        out = []
        for v in range(start, base + size, bpp):
            out.append(v)
        return out

    for idx, (cx, cz, surface_payload, deep_payload) in enumerate(rows, start=1):
        if cancel_event is not None and cancel_event.is_set():
            raise CancelledError("Cancelled during cache render.")

        if cache_mode == CACHE_MODE_ALL_BLOCKS and deep_payload is not None and (opt.y_min != cached_y_min or opt.y_max != cached_y_max):
            arrays = _project_deep_chunk_arrays(deep_payload, block_lookup, opt.y_min, opt.y_max)
        else:
            arrays = decode_surface_payload(surface_payload)

        if opt.skip_water:
            found_arr = arrays["dry_found"]
            y_arr = arrays["dry_y"]
            id_arr = arrays["dry_id"]
        else:
            found_arr = arrays["top_found"]
            y_arr = arrays["top_y"]
            id_arr = arrays["top_id"]

        base_x = cx * 16
        base_z = cz * 16
        xs = iter_sample_positions(base_x, 16, min_x)
        zs = iter_sample_positions(base_z, 16, min_z)

        for wx in xs:
            if wx < min_x or wx > max_x:
                continue
            ix = (wx - min_x) // bpp
            lx = wx - base_x
            for wz in zs:
                if wz < min_z or wz > max_z:
                    continue
                iz = (wz - min_z) // bpp
                lz = wz - base_z
                if int(found_arr[lz, lx]):
                    block_id = int(id_arr[lz, lx])
                    raw = block_lookup[block_id] if 0 <= block_id < len(block_lookup) else "minecraft:air"
                    _ep = opt.editor_palette if opt.use_editor_palette else None
                    rgb_px, norm_id, is_known, _reason = classify_block(raw, _ep)
                    if not is_known:
                        unknown_norm_counts[norm_id] += 1
                    rgb[iz, ix, :] = rgb_px
                    hmap[iz, ix] = int(y_arr[lz, lx])
                    colored_cols += 1
                else:
                    rgb[iz, ix, :] = (0, 0, 0)
                    hmap[iz, ix] = opt.y_min
                    air_only_cols += 1

        chunks_rendered += 1
        if progress_cb is not None:
            progress_cb(idx, chunks_total, idx / max(1, chunks_total))

    if opt.hillshade_mode != "none":
        shade = compute_hillshade(hmap, opt.hillshade_mode)
        altitude = compute_altitude_tint(hmap, opt.hillshade_mode)
        rgb_f = rgb.astype(np.float32) * shade[..., None] * altitude
        rgb = np.clip(rgb_f, 0, 255).astype(np.uint8)

    img = Image.fromarray(rgb, mode="RGB")
    img = fit_to_target(img, target)
    img_w, img_h = img.size
    _atomic_save_png(img, out_png, log=log_cb)
    if log_cb is not None:
        log_cb(f"Rendered from cache: {cache_path}")

    # Write per-frame unknowns JSON so worker_run can aggregate it.
    if unknown_norm_counts:
        unknown_json_path = os.path.splitext(out_png)[0] + "_unknowns.json"
        _write_frame_unknowns_json(unknown_json_path, unknown_norm_counts)

    return (min_x, max_x, min_z, max_z, chunks_rendered, 0, colored_cols, air_only_cols, bpp, img_w, img_h)


def render_snapshot_input(
    snapshot: SnapshotInput,
    out_png: str,
    opt: RenderOptions,
    log_cb: Optional[Callable[[str], None]] = None,
    progress_cb: Optional[Callable[[int, int, float], None]] = None,
    cancel_event: Optional[threading.Event] = None,
    debug_snapshot_path: Optional[str] = None,
    debug_context_header: str = "",
    stage_cb: Optional[Callable[[str], None]] = None,
) -> Tuple[int, int, int, int, int, int, int, int, int]:
    source_path = snapshot.path
    if is_cache_file(source_path):
        try:
            return render_cached_world_map(
                source_path,
                out_png,
                opt,
                log_cb=log_cb,
                progress_cb=progress_cb,
                cancel_event=cancel_event,
            )
        except RuntimeError as exc:
            # If the cache is surface-only but the requested Y range differs,
            # try falling back to an all-blocks sidecar or the raw world source.
            exc_text = str(exc).lower()
            if "top blocks" in exc_text or "surface" in exc_text:
                raw_src = snapshot.raw_path
                allblocks_path = sidecar_cache_path(
                    raw_src if raw_src else source_path,
                    opt.dimension,
                    CACHE_MODE_ALL_BLOCKS,
                )
                if os.path.isfile(allblocks_path):
                    if log_cb:
                        log_cb(
                            f"[FALLBACK] Surface cache incompatible with Y=[{opt.y_min},{opt.y_max}]. "
                            f"Using all-blocks cache: {os.path.basename(allblocks_path)}"
                        )
                    return render_cached_world_map(
                        allblocks_path,
                        out_png,
                        opt,
                        log_cb=log_cb,
                        progress_cb=progress_cb,
                        cancel_event=cancel_event,
                    )
                if raw_src and (raw_src.lower().endswith(".zip") or is_world_folder(raw_src)):
                    if log_cb:
                        log_cb(
                            f"[FALLBACK] No all-blocks cache available. "
                            f"Rendering directly from: {os.path.basename(raw_src)}"
                        )
                    fallback_snap = SnapshotInput(
                        kind=snapshot.kind,
                        path=raw_src,
                        display_name=snapshot.display_name,
                        sort_name=snapshot.sort_name,
                    )
                    return render_snapshot_input(
                        fallback_snap,
                        out_png,
                        opt,
                        log_cb=log_cb,
                        progress_cb=progress_cb,
                        cancel_event=cancel_event,
                        debug_snapshot_path=debug_snapshot_path,
                        debug_context_header=debug_context_header,
                        stage_cb=stage_cb,
                    )
            # If cache exists but has no chunks for the selected crop area,
            # try raw source (if available) instead of failing the frame.
            if "no cached chunks found" in exc_text:
                raw_src = snapshot.raw_path
                if raw_src and (raw_src.lower().endswith(".zip") or is_world_folder(raw_src)):
                    if log_cb:
                        log_cb(
                            f"[FALLBACK] Cache has no chunks for selected area. "
                            f"Rendering directly from: {os.path.basename(raw_src)}"
                        )
                    fallback_snap = SnapshotInput(
                        kind=snapshot.kind,
                        path=raw_src,
                        display_name=snapshot.display_name,
                        sort_name=snapshot.sort_name,
                    )
                    return render_snapshot_input(
                        fallback_snap,
                        out_png,
                        opt,
                        log_cb=log_cb,
                        progress_cb=progress_cb,
                        cancel_event=cancel_event,
                        debug_snapshot_path=debug_snapshot_path,
                        debug_context_header=debug_context_header,
                        stage_cb=stage_cb,
                    )
            raise

    if stage_cb is not None:
        stage_cb("raw.resolve_world_roots.start")
    with tempfile.TemporaryDirectory() as tmpdir:
        _extract_root, candidates = _resolve_snapshot_world_roots(source_path, tmpdir)
        if stage_cb is not None:
            stage_cb(f"raw.resolve_world_roots.done candidates={len(candidates)}")
        if not candidates:
            raise RuntimeError("Could not find a world folder (no level.dat found).")
        last_error: Optional[Exception] = None
        for world_root in candidates:
            try:
                if stage_cb is not None:
                    stage_cb(f"raw.world_candidate.start root={os.path.basename(world_root)}")
                return render_world_map(
                    world_root,
                    out_png,
                    opt,
                    log_cb=log_cb,
                    progress_cb=progress_cb,
                    cancel_event=cancel_event,
                    debug_snapshot_path=debug_snapshot_path,
                    debug_context_header=debug_context_header,
                    stage_cb=stage_cb,
                )
            except Exception as exc:
                last_error = exc
                if stage_cb is not None:
                    stage_cb(f"raw.world_candidate.error {type(exc).__name__}")
        if last_error is not None:
            raise RuntimeError(f"Could not load any candidate world root: {last_error}") from last_error
        raise RuntimeError("Could not load any candidate world root.")


def render_world_map(
    world_root: str,
    out_png: str,
    opt: RenderOptions,
    log_cb: Optional[Callable[[str], None]] = None,
    progress_cb: Optional[Callable[[int, int, float], None]] = None,
    cancel_event: Optional[threading.Event] = None,
    debug_snapshot_path: Optional[str] = None,
    debug_context_header: str = "",
    stage_cb: Optional[Callable[[str], None]] = None,
) -> Tuple[int, int, int, int, int, int, int, int, int]:
    world0 = None
    try:
        if amulet is None or not hasattr(amulet, "load_level"):
            raise RuntimeError(
                "Amulet API unavailable. Install/upgrade 'amulet-core' in the active environment. "
                "Expected: amulet.load_level(...)"
            )
        if stage_cb is not None:
            stage_cb("raw.world_open.start")
        world0 = amulet.load_level(world_root)
        if stage_cb is not None:
            stage_cb("raw.world_open.done")
        is_bedrock_world = os.path.isdir(os.path.join(world_root, "db"))
    
        samples_raw: List[str] = []
        colored_cols = 0
        air_only_cols = 0
        unknown_cols = 0
        unknown_norm_counts: Counter = Counter()
        unknown_raw_counts: Counter = Counter()
        exact_key_miss_counts: Counter = Counter()
        base_id_miss_counts: Counter = Counter()
    
    
        try:
            if stage_cb is not None:
                stage_cb("raw.chunk_discovery.start")
            dim_id = resolve_dimension_id(world0, opt.dimension)
            chunk_coords = world_all_chunk_coords(world0, dim_id)
            if stage_cb is not None:
                stage_cb(f"raw.chunk_discovery.done count={len(chunk_coords)}")
            if not chunk_coords:
                if log_cb:
                    log_cb(f"[ERROR] No chunks found in dimension '{opt.dimension}'. World may be empty or unexplored.")
                raise RuntimeError("No chunks found in this dimension (world may be empty/unexplored).")

            # Apply optional render limiting (block X/Z rectangle). This greatly speeds up rendering
            # and focuses on the important area.
            if opt.limit_enabled:
                if stage_cb is not None:
                    stage_cb("raw.chunk_filter.start")
                x1 = int(min(opt.x_min, opt.x_max))
                x2 = int(max(opt.x_min, opt.x_max))
                z1 = int(min(opt.z_min, opt.z_max))
                z2 = int(max(opt.z_min, opt.z_max))

                min_cx = x1 // 16 if x1 >= 0 else -((-x1 - 1) // 16) - 1
                max_cx = x2 // 16 if x2 >= 0 else -((-x2 - 1) // 16) - 1
                min_cz = z1 // 16 if z1 >= 0 else -((-z1 - 1) // 16) - 1
                max_cz = z2 // 16 if z2 >= 0 else -((-z2 - 1) // 16) - 1

                chunk_coords = [
                    (cx, cz)
                    for (cx, cz) in chunk_coords
                    if (min_cx <= cx <= max_cx and min_cz <= cz <= max_cz)
                ]
                if stage_cb is not None:
                    stage_cb(f"raw.chunk_filter.done count={len(chunk_coords)}")
                if not chunk_coords:
                    if log_cb:
                        log_cb(f"[ERROR] No chunks found within selected coordinate limits: x=[{x1},{x2}], z=[{z1},{z2}]. Try widening the X/Z bounds or disable 'Limit render area'.")
                    raise RuntimeError(
                        "No chunks found within the selected coordinate limits. "
                        "Try widening the X/Z bounds or disable 'Limit render area'."
                    )
    
            chunk_coords.sort(key=lambda t: (t[1], t[0]))
            chunks_total = len(chunk_coords)
    
            min_cx = min(c[0] for c in chunk_coords)
            max_cx = max(c[0] for c in chunk_coords)
            min_cz = min(c[1] for c in chunk_coords)
            max_cz = max(c[1] for c in chunk_coords)
    
            min_x = min_cx * 16
            max_x = (max_cx + 1) * 16 - 1
            min_z = min_cz * 16
            max_z = (max_cz + 1) * 16 - 1
    
            width_blocks = max_x - min_x + 1
            height_blocks = max_z - min_z + 1
    
            target = parse_target_preset(opt.target_preset)
            bpp = compute_blocks_per_pixel(width_blocks, height_blocks, target)
            w_px = int(np.ceil(width_blocks / bpp))
            h_px = int(np.ceil(height_blocks / bpp))
    
            rgb = np.zeros((h_px, w_px, 3), dtype=np.uint8)
            hmap = np.full((h_px, w_px), opt.y_min, dtype=np.int16)
    
            chunks_rendered = 0
            chunks_skipped = 0
    
            debug_enabled = bool(opt.debug_block_samples and debug_snapshot_path)
            debug_written_once = False
            debug_last_write = time.time()
            DEBUG_WRITE_INTERVAL_SEC = 2.0
            DEBUG_FORCE_FIRST_WRITE_AFTER_SEC = 4.0
            render_start = time.time()
    
            samples_set = set()
            max_samples = 80
    
            counters_lock = threading.Lock()
            progress_lock = threading.Lock()
    
            shared_counters = {
                "processed_chunks": 0,
                "chunks_rendered": 0,
                "chunks_skipped": 0,
                "colored_cols": 0,
                "air_only_cols": 0,
                "unknown_cols": 0,
            }
            progress_state = {"last_progress_emit": 0.0}
    
            def maybe_write_debug_snapshot(force: bool = False):
                nonlocal debug_written_once, debug_last_write
                if not debug_enabled:
                    return
                now = time.time()
                if force or (now - debug_last_write) >= DEBUG_WRITE_INTERVAL_SEC:
                    write_debug_snapshot(
                        debug_snapshot_path,
                        debug_context_header,
                        samples_raw,
                        colored_cols,
                        air_only_cols,
                        unknown_cols,
                        unknown_norm_counts,
                        unknown_raw_counts,
                    )
                    debug_last_write = now
                    debug_written_once = True
    
            n_workers = max(1, min(int(opt.workers), max(1, (os.cpu_count() or 1))))
            parts: List[List[Tuple[int, int]]] = [[] for _ in range(n_workers)]
            for i, coord in enumerate(chunk_coords):
                parts[i % n_workers].append(coord)
    
            def iter_sample_positions(base: int, size: int, min_axis: int) -> List[int]:
                offset = (min_axis - base) % bpp
                start = base + offset
                out = []
                for v in range(start, base + size, bpp):
                    out.append(v)
                return out
    
            # Shared world handle: do NOT open the world per worker.
            # Bedrock LevelDB takes an exclusive lock on db/CURRENT on open, so opening the same
            # extracted world folder in parallel workers causes:
            #   LevelDBException: ... The process cannot access the file because it is being used by another process.
            # We therefore reuse the already-open world0 and a shared chunk getter.
            get_chunk_fn_shared = probe_chunk_getter(world0, dim_id, chunk_coords)
            sample_cx, sample_cz = chunk_coords[0]
            sample_wx = sample_cx * 16
            sample_wz = sample_cz * 16
            world_block_getter = probe_world_block_getter(world0, dim_id, (sample_wx, sample_wz), opt.y_max)
    
            chunk_access_lock = None
            try:
                lw = getattr(world0, 'level_wrapper', None)
                if lw is not None and ('LevelDB' in type(lw).__name__ or 'leveldb' in str(type(lw)).lower()):
                    # Some Bedrock worlds are happiest when the DB read handle is serialized.
                    # We still parallelize the expensive per-column scan.
                    chunk_access_lock = threading.Lock()
            except Exception:
                chunk_access_lock = None
    
            # LRU cache for chunk reads (per render)
            def chunk_cache_key(cx, cz):
                return f"{cx},{cz}"

            @lru_cache(maxsize=128)
            def get_chunk_cached(cx, cz):
                return get_chunk_fn_shared(cx, cz)


            from worker_fn_module import worker_fn, iter_sample_positions
    
            if stage_cb is not None:
                stage_cb(f"raw.chunk_scan.start chunks={chunks_total} workers={n_workers}")
            with ThreadPoolExecutor(max_workers=n_workers) as ex:
                futures = [
                    ex.submit(
                        worker_fn,
                        i, part,
                        min_x, max_x, min_z, max_z, bpp,
                        get_chunk_cached, chunk_access_lock, cancel_event, opt,
                        rgb, hmap, samples_set, samples_raw, max_samples,
                        debug_enabled, render_start, DEBUG_FORCE_FIRST_WRITE_AFTER_SEC,
                        maybe_write_debug_snapshot, progress_cb, progress_lock, progress_state, chunks_total,
                        counters_lock, shared_counters,
                        PALETTE_KEY_TO_IDX, PALETTE_COLOR_TABLE,
                        chunks_rendered, chunks_skipped, colored_cols, air_only_cols, unknown_cols,
                        unknown_norm_counts, unknown_raw_counts, exact_key_miss_counts, base_id_miss_counts,
                        world_block_getter,
                        find_top_block_in_column,
                        iter_sample_positions,
                        classify_block,
                    )
                    for i, part in enumerate(parts)
                ]
                for fut in as_completed(futures):
                    if cancel_event is not None and cancel_event.is_set():
                        break
                    fut.result()
            if stage_cb is not None:
                stage_cb("raw.chunk_scan.done")

            chunks_rendered = int(shared_counters.get("chunks_rendered", 0))
            chunks_skipped = int(shared_counters.get("chunks_skipped", 0))
            colored_cols = int(shared_counters.get("colored_cols", 0))
            air_only_cols = int(shared_counters.get("air_only_cols", 0))
            unknown_cols = int(shared_counters.get("unknown_cols", 0))
    
            if cancel_event is not None and cancel_event.is_set():
                maybe_write_debug_snapshot(force=True)
                raise CancelledError("Cancelled during rendering.")
    
            maybe_write_debug_snapshot(force=True)
    
            if chunks_rendered == 0:
                raise RuntimeError("No chunks could be loaded (all chunk fetches failed).")
    
            if progress_cb is not None:
                progress_cb(chunks_total, chunks_total, 1.0)
    
            if opt.hillshade_mode != "none":
                shade = compute_hillshade(hmap, opt.hillshade_mode)
                altitude = compute_altitude_tint(hmap, opt.hillshade_mode)
                rgb_f = rgb.astype(np.float32) * shade[..., None] * altitude
                rgb = np.clip(rgb_f, 0, 255).astype(np.uint8)
    
            img = Image.fromarray(rgb, mode="RGB")
            img = fit_to_target(img, target)
            img_w, img_h = img.size
            if stage_cb is not None:
                stage_cb("raw.image_save.start")
            img.save(out_png)
            if stage_cb is not None:
                stage_cb("raw.image_save.done")

            if log_cb and debug_enabled:
                log_cb(f"  Debug snapshot file: {debug_snapshot_path}")
                log_cb(f"  Column scan results: colored={colored_cols} air_only={air_only_cols} unknown_colored={unknown_cols}")
                if unknown_cols > 0:
                    top = unknown_norm_counts.most_common(10)
                    log_cb("  Top unknown normalized IDs (rendered gray):")
                    for bid, cnt in top:
                        log_cb(f"    {cnt:8d}  {bid}")

            # Write a machine-readable per-frame unknowns JSON alongside the frame PNG
            # (always, even if debug_block_samples is off) so worker_run can aggregate them.
            if unknown_norm_counts:
                unknown_json_path = os.path.splitext(out_png)[0] + "_unknowns.json"
                _write_frame_unknowns_json(unknown_json_path, unknown_norm_counts)

            return (min_x, max_x, min_z, max_z, chunks_rendered, chunks_skipped, colored_cols, air_only_cols, bpp, img_w, img_h)
    
        except Exception:
            if opt.debug_block_samples and debug_snapshot_path:
                try:
                    write_debug_snapshot(
                        debug_snapshot_path,
                        debug_context_header,
                        samples_raw,
                        colored_cols,
                        air_only_cols,
                        unknown_cols,
                        unknown_norm_counts,
                        unknown_raw_counts,
                    )
                except Exception:
                    pass
            raise
        finally:
            try:
                world0.close()
            except Exception:
                pass
    
    
    finally:
        try:
            if world0 is not None:
                world0.close()
        except Exception:
            pass
        # Encourage release of LevelDB/Anvil handles on Windows before cleanup
        world0 = None
        try:
            import gc as _gc
            _gc.collect()
        except Exception:
            pass
def choose_debug_chunk(coords: List[Tuple[int, int]]) -> Tuple[int, int]:
    if (0, 0) in coords:
        return (0, 0)
    coords_sorted = sorted(coords, key=lambda t: (t[1], t[0]))
    return coords_sorted[len(coords_sorted) // 2]


def render_one_chunk_png(
    world_root: str,
    out_png: str,
    opt: RenderOptions,
    chunk_coord: Tuple[int, int],
    debug_txt: str,
    header: str,
    cancel_event: threading.Event,
    debug_scale: int = 16,
) -> None:
    w = amulet.load_level(world_root)
    samples_raw: List[str] = []
    colored_cols = 0
    air_only_cols = 0
    unknown_cols = 0
    unknown_norm_counts: Counter = Counter()

    try:
        dim_id = resolve_dimension_id(w, opt.dimension)
        all_coords = world_all_chunk_coords(w, dim_id)
        if not all_coords:
            raise RuntimeError("No chunks found in this dimension.")

        get_chunk_fn = probe_chunk_getter(w, dim_id, all_coords)

        cx, cz = chunk_coord
        try:
            ch = get_chunk_fn(cx, cz)
        except Exception as e:
            raise RuntimeError(f"Could not load chosen chunk {chunk_coord}: {type(e).__name__}: {e}") from e

        rgb = np.zeros((16, 16, 3), dtype=np.uint8)
        hmap = np.full((16, 16), opt.y_min, dtype=np.int16)

        samples_set = set()
        max_samples = 150

        for lx in range(16):
            if cancel_event and cancel_event.is_set():
                raise CancelledError("Cancelled during debug chunk render.")
            for lz in range(16):
                if cancel_event and cancel_event.is_set():
                    raise CancelledError("Cancelled during debug chunk render.")

                found, top_y, raw_id = find_top_block_in_column(
                    ch, lx, lz, opt.y_min, opt.y_max, opt.skip_water, opt.fast_scan
                )

                if found:
                    _ep = opt.editor_palette if opt.use_editor_palette else None
                    rgb_px, norm_id, is_known, _ = classify_block(raw_id, _ep)
                    colored_cols += 1
                    rgb[lz, lx, :] = rgb_px
                    hmap[lz, lx] = top_y

                    if opt.debug_block_samples and len(samples_set) < max_samples:
                        if raw_id not in samples_set:
                            samples_set.add(str(raw_id))
                            samples_raw.append(str(raw_id))

                    if not is_known:
                        unknown_cols += 1
                        unknown_norm_counts[norm_id] += 1

                else:
                    air_only_cols += 1
                    rgb[lz, lx, :] = (0, 0, 0)
                    hmap[lz, lx] = opt.y_min

        write_debug_snapshot(debug_txt, header, samples_raw, colored_cols, air_only_cols, unknown_cols, unknown_norm_counts)

        if opt.hillshade_mode != "none":
            shade = compute_hillshade(hmap, opt.hillshade_mode)
            altitude = compute_altitude_tint(hmap, opt.hillshade_mode)
            rgb_f = rgb.astype(np.float32) * shade[..., None] * altitude
            rgb = np.clip(rgb_f, 0, 255).astype(np.uint8)

        img = Image.fromarray(rgb, mode="RGB").resize((16 * debug_scale, 16 * debug_scale), resample=Image.NEAREST)
        img.save(out_png)

    finally:
        try:
            w.close()
        except Exception:
            pass


def extract_index_from_name(zip_name: str) -> int:
    m = re.search(r"(\\d+)", zip_name)
    return int(m.group(1)) if m else -1


def find_zip_backups(folder: str) -> List[str]:
    zips = [os.path.join(folder, f) for f in os.listdir(folder) if f.lower().endswith(".zip")]
    if not zips:
        return []

    def sort_key(path: str):
        name = os.path.basename(path)
        idx = extract_index_from_name(name)
        if idx >= 0:
            return (0, idx, name.lower())
        return (1, 0, name.lower())

    zips_sorted = sorted(zips, key=sort_key)
    return list(reversed(zips_sorted))


def sort_frames_chronological(frames: list) -> List[str]:
    """Sort (frame_no, path[, ...]) tuples by frame_no and return paths in order."""
    frames_sorted = sorted(frames, key=lambda t: t[0])
    return [t[1] for t in frames_sorted]


# Maximum per-dimension pixel size the GIF format supports.
_GIF_MAX_DIM = 65535
# Threshold above which we log a "large canvas" advisory.
_GIF_LARGE_WARN_PX = 8000


def align_and_composite_frames(
    frames: list,   # List of (frame_no, frame_png, bounds) — bounds may be None
    aligned_dir: str,
    log_cb: Callable[[str], None],
) -> List[str]:
    """Composite all frame PNGs onto a shared geographic canvas so that block coordinate (X, Z)
    maps to the same pixel position in every frame of the timelapse.

    *bounds* = (min_x, max_x, min_z, max_z, chunks_rendered, chunks_skipped,
                colored_cols, air_only_cols, bpp, img_w_px, img_h_px)

    img_w_px / img_h_px are the *actual* dimensions of the saved PNG after any fit_to_target
    downscale, and are the source of truth for computing the pixel/block ratio.

    Returns a list of aligned PNG paths in the same order as *frames*.
    Falls back to the original paths (unaligned) if bounds are missing.
    """
    if not frames:
        return []

    bounds_list = [b for _, _, b in frames]
    if any(b is None or len(b) < 11 for b in bounds_list):
        log_cb("[ALIGN] Cannot align frames: geographic bounds unavailable for one or more frames.")
        return [p for _, p, _ in frames]

    global_min_x = min(int(b[0]) for b in bounds_list)
    global_max_x = max(int(b[1]) for b in bounds_list)
    global_min_z = min(int(b[2]) for b in bounds_list)
    global_max_z = max(int(b[3]) for b in bounds_list)

    global_blocks_x = global_max_x - global_min_x + 1
    global_blocks_z = global_max_z - global_min_z + 1

    # If all frames already share the same geographic extent, skip compositing.
    all_same = all(
        int(b[0]) == global_min_x and int(b[1]) == global_max_x
        and int(b[2]) == global_min_z and int(b[3]) == global_max_z
        for b in bounds_list
    )
    if all_same:
        log_cb("[ALIGN] All frames share the same geographic extent — no alignment needed.")
        return [p for _, p, _ in frames]

    # Each frame may have a different pixel/block ratio because:
    #   a) bpp (blocks-per-pixel pre-resize) varies across frames
    #   b) fit_to_target applies an additional fractional downscale
    # The authoritative ratio is:  px_per_block = img_w_px / (max_x - min_x + 1)
    # We pick the *smallest* px_per_block across all frames as the global target so we
    # never have to upscale any frame (only downscale or leave unchanged).
    px_per_block_ratios = []
    for b in bounds_list:
        bx = int(b[1]) - int(b[0]) + 1  # blocks_x for this frame
        bz = int(b[3]) - int(b[2]) + 1  # blocks_z for this frame
        iw = int(b[9])   # actual PNG width in pixels
        ih = int(b[10])  # actual PNG height in pixels
        rx = iw / bx if bx > 0 else 1.0
        rz = ih / bz if bz > 0 else 1.0
        # Use the minimum of x/z ratios to stay square
        px_per_block_ratios.append(min(rx, rz))

    global_px_per_block = min(px_per_block_ratios)   # smallest = coarsest

    canvas_w = max(1, int(round(global_blocks_x * global_px_per_block)))
    canvas_h = max(1, int(round(global_blocks_z * global_px_per_block)))

    # GIF format hard limit: 65535 per dimension.
    if canvas_w > _GIF_MAX_DIM or canvas_h > _GIF_MAX_DIM:
        scale_down = min(_GIF_MAX_DIM / canvas_w, _GIF_MAX_DIM / canvas_h)
        global_px_per_block *= scale_down
        canvas_w = max(1, int(round(global_blocks_x * global_px_per_block)))
        canvas_h = max(1, int(round(global_blocks_z * global_px_per_block)))
        log_cb(
            f"[ALIGN] WARNING: The combined map canvas exceeds the GIF format limit ({_GIF_MAX_DIM}px per side). "
            f"Frames will be scaled down to {canvas_w}×{canvas_h}px so the animation fits. "
            f"To avoid this, enable a resolution target such as '1920×1080 (HD)' in the settings."
        )
    elif canvas_w > _GIF_LARGE_WARN_PX or canvas_h > _GIF_LARGE_WARN_PX:
        log_cb(
            f"[ALIGN] Note: The aligned canvas is {canvas_w}×{canvas_h}px — this will produce a large GIF. "
            f"Consider using a resolution target (e.g. '1920×1080 (HD)') to limit output size."
        )

    log_cb(
        f"[ALIGN] Aligning {len(frames)} frame(s) to global canvas {canvas_w}×{canvas_h}px "
        f"(X=[{global_min_x},{global_max_x}], Z=[{global_min_z},{global_max_z}], "
        f"px/block={global_px_per_block:.4f})"
    )
    os.makedirs(aligned_dir, exist_ok=True)

    result: List[str] = []
    _prev_limit = Image.MAX_IMAGE_PIXELS
    Image.MAX_IMAGE_PIXELS = None
    try:
        for idx, (frame_no, frame_png, bounds) in enumerate(frames, start=1):
            min_x, max_x, min_z, max_z = int(bounds[0]), int(bounds[1]), int(bounds[2]), int(bounds[3])
            img_w = int(bounds[9])
            img_h = int(bounds[10])
            blocks_x = max_x - min_x + 1
            blocks_z = max_z - min_z + 1

            # Target size for this frame on the global canvas (may differ from img_w/img_h
            # if this frame was rendered at a finer scale than the global minimum).
            target_frame_w = max(1, int(round(blocks_x * global_px_per_block)))
            target_frame_h = max(1, int(round(blocks_z * global_px_per_block)))

            # Pixel offset on the canvas: how far this frame's origin is from the global origin.
            ox = int(round((min_x - global_min_x) * global_px_per_block))
            oz = int(round((min_z - global_min_z) * global_px_per_block))

            canvas = Image.new("RGB", (canvas_w, canvas_h), (0, 0, 0))
            frame_img = Image.open(frame_png).convert("RGB")

            # Resize only if needed (avoids touching frames that are already the right size).
            if (frame_img.width, frame_img.height) != (target_frame_w, target_frame_h):
                frame_img = frame_img.resize((target_frame_w, target_frame_h), Image.NEAREST)

            canvas.paste(frame_img, (ox, oz))
            frame_img.close()

            aligned_png = os.path.join(aligned_dir, os.path.basename(frame_png))
            canvas.save(aligned_png)
            canvas.close()

            log_cb(
                f"[ALIGN] {idx}/{len(frames)}: canvas=({ox},{oz}) "
                f"frame={target_frame_w}×{target_frame_h}px "
                f"[X={min_x}..{max_x}, Z={min_z}..{max_z}]"
            )
            result.append(aligned_png)
    finally:
        Image.MAX_IMAGE_PIXELS = _prev_limit

    return result



def build_gif(frame_paths: List[str], out_gif: str, seconds_per_frame: float, loop: int = 0):
    if not frame_paths:
        raise ValueError("No frames to animate.")
    # Disable Pillow's decompression-bomb guard for files we generated ourselves.
    _prev_limit = Image.MAX_IMAGE_PIXELS
    Image.MAX_IMAGE_PIXELS = None
    try:
        frames = [Image.open(p).convert("RGB") for p in frame_paths]
    finally:
        Image.MAX_IMAGE_PIXELS = _prev_limit
    duration_ms = int(round(1000.0 * max(0.05, seconds_per_frame)))

    frames[0].save(
        out_gif,
        save_all=True,
        append_images=frames[1:],
        duration=duration_ms,
        loop=loop,
        optimize=False,
        disposal=2,
    )
    for im in frames:
        try:
            im.close()
        except Exception:
            pass


class EtaSmoother:
    def __init__(self, window: int = 5):
        self.values = deque(maxlen=max(1, window))

    def add(self, v: float) -> Optional[float]:
        if v is None or v != v or v < 0:
            return self.value()
        self.values.append(float(v))
        return self.value()

    def value(self) -> Optional[float]:
        if not self.values:
            return None
        return sum(self.values) / len(self.values)


def estimate_remaining_seconds(elapsed: float, frac_done: float) -> Optional[float]:
    if frac_done <= 0.005:
        return None
    total_est = elapsed / max(1e-6, frac_done)
    rem = max(0.0, total_est - elapsed)
    return rem


def _machine_profile_key(has_psutil: bool) -> str:
    logical = int(os.cpu_count() or 1)
    physical = logical
    mem_gb = 0
    if has_psutil:
        try:
            import psutil
            physical = int(psutil.cpu_count(logical=False) or logical)
            mem_gb = int(round(float(psutil.virtual_memory().total) / (1024.0 ** 3)))
        except Exception:
            physical = logical
            mem_gb = 0
    return f"{sys.platform}|{logical}|{physical}|{mem_gb}"


def _load_auto_profile(machine_key: str) -> Dict[str, Any]:
    cfg = _load_config()
    root = cfg.get("auto_tuner", {}) if isinstance(cfg.get("auto_tuner", {}), dict) else {}
    prof = root.get(machine_key, {}) if isinstance(root.get(machine_key, {}), dict) else {}
    return prof


def _save_auto_profile(machine_key: str, profile: Dict[str, Any]) -> None:
    cfg = _load_config()
    root = cfg.get("auto_tuner", {}) if isinstance(cfg.get("auto_tuner", {}), dict) else {}
    root[machine_key] = profile
    cfg["auto_tuner"] = root
    _save_config(cfg)


def _compute_auto_strategy(has_psutil: bool, log_cb: Optional[Callable[[str], None]] = None) -> Dict[str, Any]:
    logical = int(os.cpu_count() or 1)
    physical = logical
    mem_gb = 0
    if has_psutil:
        try:
            import psutil
            physical = int(psutil.cpu_count(logical=False) or logical)
            mem_gb = int(round(float(psutil.virtual_memory().total) / (1024.0 ** 3)))
        except Exception:
            physical = logical
            mem_gb = 0

    machine_key = _machine_profile_key(has_psutil)
    profile = _load_auto_profile(machine_key)

    # Baseline heuristic tuned for this workload:
    # prefer more frame-level processes and fewer in-frame threads.
    base_workers = 1
    if logical <= 4:
        base_max_concurrency = max(1, min(2, logical - 1 if logical > 1 else 1))
    elif logical <= 8:
        base_max_concurrency = max(2, min(4, logical - 1))
    elif logical <= 16:
        base_max_concurrency = max(3, min(6, logical - 2))
    else:
        base_max_concurrency = max(4, min(8, logical - 2))

    # Keep memory pressure modest on low-RAM systems.
    if mem_gb and mem_gb <= 8:
        base_max_concurrency = max(1, min(base_max_concurrency, 3))
    elif mem_gb and mem_gb <= 12:
        base_max_concurrency = max(1, min(base_max_concurrency, 4))

    base_initial_concurrency = max(1, min(base_max_concurrency, max(1, base_max_concurrency - 1)))

    best = profile.get("best", {}) if isinstance(profile.get("best", {}), dict) else {}
    if best:
        bw = int(best.get("frame_workers", base_workers))
        bc = int(best.get("frame_concurrency", base_initial_concurrency))
        bm = int(best.get("max_concurrency", base_max_concurrency))
        base_workers = max(1, min(4, bw))
        base_max_concurrency = max(1, min(8, bm))
        base_initial_concurrency = max(1, min(base_max_concurrency, bc))

    strategy = {
        "machine_key": machine_key,
        "logical_cores": logical,
        "physical_cores": physical,
        "memory_gb": mem_gb,
        "frame_workers": base_workers,
        "initial_concurrency": base_initial_concurrency,
        "max_concurrency": base_max_concurrency,
        "min_concurrency": 1,
        "target_cpu_low": 68,
        "target_cpu_high": 90,
        "fast_scan": False,
        "aggressive_mode": True,
    }
    if log_cb:
        log_cb(
            "Auto-tuner strategy: "
            f"cores={logical} (physical={physical}), mem={mem_gb}GB, "
            f"frame_workers={strategy['frame_workers']}, "
            f"concurrency={strategy['initial_concurrency']}..{strategy['max_concurrency']}"
        )
    return strategy


def _record_auto_tuner_result(machine_key: str, strategy: Dict[str, Any], rendered: int, elapsed_s: float, failed: int) -> None:
    if rendered <= 0 or elapsed_s <= 1.0:
        return
    frames_per_min = (float(rendered) * 60.0) / max(1.0, float(elapsed_s))
    success_ratio = float(rendered) / max(1.0, float(rendered + failed))
    score = frames_per_min * max(0.25, success_ratio)

    profile = _load_auto_profile(machine_key)
    best = profile.get("best", {}) if isinstance(profile.get("best", {}), dict) else {}
    best_score = float(best.get("score", 0.0) or 0.0)

    candidate = {
        "frame_workers": int(strategy.get("frame_workers", 1)),
        "frame_concurrency": int(strategy.get("initial_concurrency", 1)),
        "max_concurrency": int(strategy.get("max_concurrency", 1)),
        "score": round(score, 4),
        "frames_per_min": round(frames_per_min, 4),
        "success_ratio": round(success_ratio, 4),
        "updated_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }

    profile["last"] = candidate
    if score >= best_score:
        profile["best"] = candidate
    _save_auto_profile(machine_key, profile)


def worker_run(snapshots: List[SnapshotInput],
    out_dir: str,
    opt: RenderOptions,
    seconds_per_frame: float,msgq: "queue.Queue[tuple]",
    cancel_event: threading.Event,
    stop_control: Optional[Dict[str, str]] = None,
    input_folder: str = "",
    output_cache_mode: str = "",
    discovery_lines: Optional[List[str]] = None,
):
    try:
        run_id = time.strftime("%Y%m%d-%H%M%S")
        run_dir = os.path.join(out_dir, f"timelapse_{run_id}")
        frames_dir = os.path.join(run_dir, "frames")
        os.makedirs(frames_dir, exist_ok=True)

        run_log_path = os.path.join(run_dir, "run.log")
        run_log = open(run_log_path, "a", encoding="utf-8", buffering=1, newline="\n")

        def log(msg: str):
            msgq.put(("log", msg))
            try:
                # Normalize any accidental escaped newlines so log files are readable.
                run_log.write(str(msg).replace("\\r\\n", "\n").replace("\\n", "\n") + "\n")
            except Exception:
                pass

        def status(line1: str, line2: str = ""):
            msgq.put(("status", (line1, line2)))

        def progress(v: float):
            msgq.put(("progress", v))

        def _force_terminate_pool(pool: Any, label: str) -> None:
            """Best-effort hard stop for ProcessPoolExecutor workers.

            Needed when a worker ignores cooperative shutdown (e.g., stuck during
            raw ZIP cleanup). Without this, orphan workers can keep high RAM/CPU.
            """
            try:
                procs = getattr(pool, "_processes", None)
                if isinstance(procs, dict) and procs:
                    for _pid, proc in list(procs.items()):
                        try:
                            if proc is not None and proc.is_alive():
                                proc.terminate()
                        except Exception:
                            pass
                    for _pid, proc in list(procs.items()):
                        try:
                            if proc is not None and proc.is_alive() and hasattr(proc, "kill"):
                                proc.kill()
                        except Exception:
                            pass
                    log(f"[POOL] Forced termination for {label} worker pool.")
            except Exception:
                pass

        log(f"{APP_NAME} v{APP_VERSION} (build {APP_BUILD})")
        log(f"Run folder: {run_dir}")
        log(f"Log file: {run_log_path}")
        if input_folder:
            log(f"Input folder: {input_folder}")
        log(f"Found {len(snapshots)} snapshots.")
        log(f"Output: {out_dir}")
        log(f"Dimension: {opt.dimension} | y=[{opt.y_min},{opt.y_max}] | seconds_per_frame={seconds_per_frame}")
        log(f"Target: {opt.target_preset} | performance mode=automatic")
        if output_cache_mode:
            log(f"Output cache mode: {output_cache_mode}")
        if opt.limit_enabled:
            log(
                f"Crop: enabled x=[{opt.x_min},{opt.x_max}] z=[{opt.z_min},{opt.z_max}]"
            )
        else:
            log("Crop: disabled")
        log("Processing order: newest snapshots first (reverse filename index).")
        log("GIF order: oldest → newest (chronological).")
        try:
            queue_preview = " -> ".join([s.display_name for s in snapshots])
            log(f"Queue (newest→oldest): {queue_preview}")
        except Exception:
            pass
        log("Rendering frames concurrently (multiple frames at once).")
        log("-" * 60)

        if discovery_lines:
            log("[DISCOVERY DETAILS]")
            for line in discovery_lines:
                try:
                    log(str(line))
                except Exception:
                    pass
            log("-" * 60)

        requested_cache_mode = str(output_cache_mode or "").strip().lower()
        if requested_cache_mode not in (CACHE_MODE_SURFACE, CACHE_MODE_ALL_BLOCKS):
            requested_cache_mode = CACHE_MODE_NONE

        if requested_cache_mode != CACHE_MODE_NONE:
            log(
                f"[CACHE OUTPUT] Prebuilding '{requested_cache_mode}' sidecar caches before render "
                "for raw snapshots when missing or mismatched."
            )
            prepared_snapshots: List[SnapshotInput] = []
            total_prep = len(snapshots)

            # Quick pre-scan (header check only) to know how many actually need building.
            _needs_build_count = 0
            for _s in snapshots:
                if not is_cache_file(_s.path):
                    _raw = _s.raw_path or _s.path
                    _cp = sidecar_cache_path(_raw, opt.dimension, requested_cache_mode)
                    _ok, _ = _cache_mismatch_reason(_cp, _raw, requested_cache_mode, opt.dimension, opt.y_min, opt.y_max)
                    if not _ok:
                        _needs_build_count += 1
            total_needs_build = max(1, _needs_build_count)
            completed_builds = [0]
            cache_phase_t0 = [time.time()]

            def _fmt_cache_eta(secs: float) -> str:
                s = max(0, int(secs))
                return f"{s // 3600:02d}:{(s % 3600) // 60:02d}:{s % 60:02d}"

            for prep_i, snap in enumerate(snapshots, start=1):
                if cancel_event.is_set():
                    log("[CACHE OUTPUT] Stop requested during cache preparation.")
                    break

                if is_cache_file(snap.path):
                    prepared_snapshots.append(snap)
                    continue

                raw_src = snap.raw_path or snap.path
                target_cache_path = sidecar_cache_path(raw_src, opt.dimension, requested_cache_mode)
                is_match, reason = _cache_mismatch_reason(
                    target_cache_path,
                    raw_src,
                    requested_cache_mode,
                    opt.dimension,
                    opt.y_min,
                    opt.y_max,
                )

                if is_match:
                    log(f"[CACHE OUTPUT] {prep_i:02d}/{total_prep:02d} using existing cache: {os.path.basename(target_cache_path)}")
                    prepared_snapshots.append(
                        SnapshotInput(
                            kind="cache",
                            path=target_cache_path,
                            display_name=snap.display_name,
                            sort_name=snap.sort_name,
                            raw_path=raw_src,
                            cache_path=target_cache_path,
                            warning=snap.warning,
                        )
                    )
                    continue

                try:
                    _build_seq = completed_builds[0] + 1
                    _build_label = snap.display_name

                    def _cache_progress_cb(
                        done: int,
                        total_chunks: int,
                        frac: float,
                        _bname: str = _build_label,
                        _bseq: int = _build_seq,
                        _btotal: int = total_needs_build,
                    ) -> None:
                        _overall = (completed_builds[0] + frac) / _btotal
                        progress(_overall * 100.0)
                        _elapsed = time.time() - cache_phase_t0[0]
                        if _overall > 0.02 and _elapsed > 2.0:
                            _rem = _elapsed / _overall - _elapsed
                            _eta = _fmt_cache_eta(_rem)
                        else:
                            _eta = "--"
                        status(
                            f"Building caches ({_bseq}/{_btotal}): {_bname}",
                            f"Chunk {done}/{total_chunks} — Cache build ETA {_eta}",
                        )

                    status(
                        f"Building caches ({_build_seq}/{total_needs_build}): {snap.display_name}",
                        "Starting… (loading world)",
                    )
                    progress((completed_builds[0] / total_needs_build) * 100.0)
                    log(
                        f"[CACHE OUTPUT] {prep_i:02d}/{total_prep:02d} building cache for {snap.display_name} "
                        f"({reason})"
                    )
                    built_cache_path = build_snapshot_cache(
                        snapshot=snap,
                        cache_mode=requested_cache_mode,
                        dimension=opt.dimension,
                        y_min=opt.y_min,
                        y_max=opt.y_max,
                        log_cb=lambda m: log(f"[CACHE OUTPUT] {m}"),
                        progress_cb=_cache_progress_cb,
                        cancel_event=cancel_event,
                    )
                    completed_builds[0] += 1
                    prepared_snapshots.append(
                        SnapshotInput(
                            kind="cache",
                            path=built_cache_path,
                            display_name=snap.display_name,
                            sort_name=snap.sort_name,
                            raw_path=raw_src,
                            cache_path=built_cache_path,
                            warning=snap.warning,
                        )
                    )
                    log(f"[CACHE OUTPUT] Built cache for {snap.display_name}: {os.path.basename(built_cache_path)}")
                except CancelledError:
                    log("[CACHE OUTPUT] Cache preparation cancelled by user.")
                    break
                except Exception as exc:
                    log(
                        f"[CACHE OUTPUT] Cache build failed for {snap.display_name}: "
                        f"{type(exc).__name__}: {exc}. Falling back to raw render for this snapshot."
                    )
                    prepared_snapshots.append(snap)

            if prepared_snapshots:
                snapshots = prepared_snapshots
            status("Rendering frames…", "")
            progress(0.0)
            log("-" * 60)

        # Cache preflight diagnostics: show what each snapshot will likely read from.
        try:
            log("[CACHE PREFLIGHT]")
            cache_backed = 0
            raw_only = 0
            for i, snap in enumerate(snapshots, start=1):
                src_name = snap.display_name
                if is_cache_file(snap.path):
                    cache_backed += 1
                    mode = "?"
                    y_lo = "?"
                    y_hi = "?"
                    try:
                        h = read_cache_header(snap.path)
                        mode = str(h.get("cache_mode", "?"))
                        y_lo = str(h.get("y_min", "?"))
                        y_hi = str(h.get("y_max", "?"))
                    except Exception:
                        pass
                    log(
                        f"  {i:02d}. {src_name} -> cache ({os.path.basename(snap.path)}) "
                        f"mode={mode} y=[{y_lo},{y_hi}]"
                    )
                else:
                    raw_only += 1
                    raw_src = snap.raw_path or snap.path
                    surf_path = sidecar_cache_path(raw_src, opt.dimension, CACHE_MODE_SURFACE)
                    allb_path = sidecar_cache_path(raw_src, opt.dimension, CACHE_MODE_ALL_BLOCKS)
                    surf_ok, surf_reason = _cache_mismatch_reason(
                        surf_path, raw_src, CACHE_MODE_SURFACE, opt.dimension, opt.y_min, opt.y_max
                    )
                    allb_ok, allb_reason = _cache_mismatch_reason(
                        allb_path, raw_src, CACHE_MODE_ALL_BLOCKS, opt.dimension, opt.y_min, opt.y_max
                    )
                    raw_dir = os.path.dirname(raw_src)
                    raw_stem = snapshot_stem(raw_src).lower()

                    # Capture current source signature used for cache matching.
                    src_sig = None
                    try:
                        src_sig = build_source_signature(raw_src)
                    except Exception:
                        src_sig = None

                    # Show all related cache files in the same folder for quick forensic checks.
                    related_cache_files: List[str] = []
                    try:
                        for nm in sorted(os.listdir(raw_dir), key=lambda s: s.lower()):
                            low = nm.lower()
                            if not low.endswith(".wmtt4mc"):
                                continue
                            if raw_stem in snapshot_stem(os.path.join(raw_dir, nm)).lower():
                                related_cache_files.append(nm)
                    except Exception:
                        pass

                    log(
                        f"  {i:02d}. {src_name} -> raw ({os.path.basename(raw_src)}) "
                        f"cache_match(surface={str(bool(surf_ok)).lower()}:{surf_reason}, "
                        f"all_blocks={str(bool(allb_ok)).lower()}:{allb_reason})"
                    )
                    log(f"      expected_surface={surf_path}")
                    log(f"      expected_all_blocks={allb_path}")
                    if related_cache_files:
                        log(f"      related_cache_files={', '.join(related_cache_files)}")
                    else:
                        log("      related_cache_files=(none)")

                    if src_sig is not None:
                        log(
                            "      source_signature="
                            f"kind={src_sig.get('source_kind')} "
                            f"name={src_sig.get('source_name')} "
                            f"size={src_sig.get('source_size')} "
                            f"mtime_ns={src_sig.get('source_mtime_ns')}"
                        )

                    def _log_cache_header_details(label: str, path: str) -> None:
                        if not os.path.isfile(path):
                            return
                        try:
                            h = read_cache_header(path)
                            log(
                                f"      {label}_header="
                                f"mode={h.get('cache_mode')} "
                                f"dimension={h.get('dimension')} "
                                f"y=[{h.get('y_min')},{h.get('y_max')}] "
                                f"source_kind={h.get('source_kind')} "
                                f"source_name={h.get('source_name')} "
                                f"source_size={h.get('source_size')} "
                                f"source_mtime_ns={h.get('source_mtime_ns')}"
                            )
                        except Exception as exc:
                            log(f"      {label}_header=read_error:{type(exc).__name__}")

                    _log_cache_header_details("surface", surf_path)
                    _log_cache_header_details("all_blocks", allb_path)
            log(
                f"  Summary: cache-backed={cache_backed}, raw-only={raw_only}, total={len(snapshots)}"
            )
            if raw_only > 0:
                log(
                    "  NOTE: raw-only snapshots can be much slower and are more likely to hit worker stalls "
                    "than cache-backed snapshots."
                )
        except Exception as _cache_diag_exc:
            log(f"[CACHE PREFLIGHT] Warning: {type(_cache_diag_exc).__name__}: {_cache_diag_exc}")

        log("-" * 60)

        rendered_frames: List[Tuple[int, str, Any]] = []  # (frame_no, frame_png, bounds)
        skipped: List[Tuple[str, str, str]] = []

        zip_times: List[float] = []
        cache_zip_times: List[float] = []
        total_zips = len(snapshots)
        total_raw_snapshots = sum(1 for s in snapshots if is_cache_file(s.path) is False)
        total_cache_snapshots = max(0, total_zips - total_raw_snapshots)
        chronological = list(reversed(snapshots))  # oldest -> newest
        frame_no_by_path = {snap.path: i+1 for i, snap in enumerate(chronological)}

        # Build display-name-based PNG paths, deduplicating when multiple snapshots share the same display name.
        _dn_total: Dict[str, int] = {}
        for snap in chronological:
            key = safe_filename(snap.display_name) or f"frame_{frame_no_by_path[snap.path]:04d}"
            _dn_total[key] = _dn_total.get(key, 0) + 1
        _dn_used: Dict[str, int] = {}
        frame_png_by_path: Dict[str, str] = {}
        for i, snap in enumerate(chronological):
            key = safe_filename(snap.display_name) or f"frame_{i+1:04d}"
            if _dn_total.get(key, 1) > 1:
                _dn_used[key] = _dn_used.get(key, 0) + 1
                png_name = f"{key}_{_dn_used[key]:04d}.png"
            else:
                png_name = f"{key}.png"
            frame_png_by_path[snap.path] = os.path.join(frames_dir, png_name)
        RENDER_WEIGHT = 92.0

        # --- Block-based progress setup ---
        # Estimate total blocks to render across all frames using cheap metadata scans.
        total_blocks = 0
        frame_block_counts = {}
        known_areas: List[int] = []
        # Use crop area as an upper bound when crop is enabled.
        if opt.limit_enabled:
            x1 = int(min(opt.x_min, opt.x_max))
            x2 = int(max(opt.x_min, opt.x_max))
            z1 = int(min(opt.z_min, opt.z_max))
            z2 = int(max(opt.z_min, opt.z_max))
        else:
            # If not limited, use a large default (e.g., 5120x5120)
            x1, x2, z1, z2 = 0, 5119, 0, 5119
        width = abs(x2 - x1) + 1
        height = abs(z2 - z1) + 1
        crop_area = width * height

        for snapshot in chronological:
            est, est_source = estimate_snapshot_block_area_with_source(snapshot, opt.dimension)
            if est is not None and est > 0:
                if opt.limit_enabled:
                    est = min(int(est), int(crop_area))
                frame_block_counts[snapshot.path] = int(est)
                known_areas.append(int(est))
            try:
                if est is not None and est > 0:
                    log(f"[ESTIMATE] {snapshot.display_name}: source={est_source}, area≈{int(est):,} blocks")
                else:
                    log(f"[ESTIMATE] {snapshot.display_name}: source={est_source}, area=unknown (using default)")
            except Exception:
                pass

        default_area = int(crop_area)
        if (not opt.limit_enabled) and known_areas:
            # Median known area is more stable than a hard-coded giant default.
            s = sorted(known_areas)
            default_area = int(s[len(s) // 2])
        elif (not opt.limit_enabled) and (not known_areas):
            default_area = 5120 * 5120

        for snapshot in chronological:
            if snapshot.path not in frame_block_counts:
                frame_block_counts[snapshot.path] = int(default_area)
            total_blocks += int(frame_block_counts[snapshot.path])

        try:
            log(
                f"Block estimate: total≈{int(total_blocks):,} across {total_zips} frame(s) "
                f"(known={len(known_areas)}, default={int(default_area):,}/frame)"
            )
        except Exception:
            pass
        blocks_rendered = 0
        dynamic_area_refined = False
        job_start = time.time()
        last_error_reason = ""
        repeated_error_count = 0
        repeated_error_abort = False
        
        # --- Concurrent frame rendering setup ---
        try:
            import psutil
            has_psutil = True
        except ImportError:
            has_psutil = False
            log("(psutil not available; using fixed concurrency)")

        strategy = _compute_auto_strategy(has_psutil, log_cb=log)
        opt.workers = int(strategy["frame_workers"])
        opt.fast_scan = bool(strategy.get("fast_scan", False))
        opt.aggressive_mode = bool(strategy.get("aggressive_mode", True))

        cpu_count = int(strategy["logical_cores"])
        current_concurrency = int(strategy["initial_concurrency"])
        min_concurrency = int(strategy["min_concurrency"])
        max_concurrency = int(strategy["max_concurrency"])
        target_cpu_low = int(strategy["target_cpu_low"])
        target_cpu_high = int(strategy["target_cpu_high"])

        log(
            f"Auto performance: frame_workers={opt.workers}, "
            f"concurrent_frames={current_concurrency}..{max_concurrency}, "
            f"target_cpu={target_cpu_low}-{target_cpu_high}%"
        )
        log(f"CPU cores: {cpu_count}, initial concurrent frames: {current_concurrency} (max: {max_concurrency})")

        # Source-aware frame worker sizing: raw scans need more intra-frame workers
        # than cache-backed snapshots.
        raw_frame_workers = max(2, min(8, max(1, cpu_count // 2)))
        log(f"Source-aware workers: cache={max(1, int(opt.workers))}, raw={raw_frame_workers}")

        # Shared state for concurrent rendering
        frame_lock = threading.Lock()
        # in_flight_frames: future -> (snapshot, zip_i, start_time, source_label, stage_path)
        in_flight_frames: Dict[object, Tuple[Any, int, float, str, str]] = {}
        pending_zips_queue: deque = deque([(snapshot, i+1) for i, snapshot in enumerate(snapshots)])
        
        last_cpu_check = time.time()
        cpu_check_interval = 2.0
        cpu_samples: deque = deque(maxlen=6)
        last_concurrency_change = 0.0
        concurrency_cooldown = 4.0
        overall_eta_smoother = EtaSmoother(window=5)
        # More conservative startup frame ETA (120 sec = 2 min, for first few frames with cache loading overhead)
        startup_frame_eta_guess_s = 120.0
        # Track frame render times with separate smoothers for better ETA after startup
        first_frame_eta_smoother = EtaSmoother(window=3)  # First build/learning phase
        steady_state_eta_smoother = EtaSmoother(window=8)  # After 2-3 frames
        stuck_frame_check_interval = 15.0  # seconds
        last_stuck_check = time.time()
        STUCK_FRAME_THRESHOLD = 2 * 60 * 60  # 2 hours in seconds
        # Guard against long "98%" hangs: trigger early fallback when no future completes.
        # We start conservative, then adapt once we have observed frame times.
        NO_COMPLETION_STALL_THRESHOLD = 8 * 60  # baseline 8 minutes
        last_completion_t = time.time()
        force_early_shutdown = False
        stalled_retry_items: List[Tuple[SnapshotInput, int, str]] = []
        queued_retry_items: List[Tuple[SnapshotInput, int, str]] = []
        RAW_INFLIGHT_LIMIT = 1
        raw_zip_times: List[float] = []

        frame_executor = ProcessPoolExecutor(max_workers=max_concurrency)
        _register_process_pool(frame_executor)
        try:
            def _stage_file_path(zip_i: int, display_name: str) -> str:
                return os.path.join(run_dir, f"frame_stage_{zip_i:03d}_{safe_filename(display_name)}.txt")

            def _read_last_stage(stage_path: str) -> str:
                try:
                    with open(stage_path, "r", encoding="utf-8") as sf:
                        line = sf.read().strip()
                    if not line:
                        return "unknown"
                    parts = line.split("\t", 1)
                    return parts[1] if len(parts) == 2 else line
                except Exception:
                    return "unknown"

            def _stage_age_seconds(stage_path: str) -> Optional[float]:
                try:
                    st = os.stat(stage_path)
                    return max(0.0, time.time() - float(st.st_mtime))
                except Exception:
                    return None

            def _parse_stage_progress(stage: str) -> Optional[Tuple[int, int]]:
                try:
                    s = str(stage or "").strip().lower()
                    if not s.startswith("raw.chunk_scan.progress"):
                        return None
                    tail = s.rsplit(" ", 1)[-1]
                    parts = tail.split("/", 1)
                    if len(parts) != 2:
                        return None
                    done = int(parts[0])
                    total = int(parts[1])
                    if total <= 0:
                        return None
                    return max(0, done), max(1, total)
                except Exception:
                    return None

            def submit_frame(snapshot: SnapshotInput, zip_i: int):
                """Submit a single frame render task, return Future"""
                name = snapshot.display_name
                frame_no = frame_no_by_path.get(snapshot.path, zip_i)
                frame_png = frame_png_by_path.get(snapshot.path, os.path.join(frames_dir, f"frame_{zip_i:04d}.png"))
                debug_snapshot_path = os.path.join(run_dir, f"debug_block_ids_{zip_i:03d}_{safe_filename(name)}.txt")
                stage_path = _stage_file_path(zip_i, name)
                src_label = "cache" if is_cache_file(snapshot.path) else "raw"
                opt_for_task = clone_render_options(opt)
                if src_label == "raw":
                    opt_for_task.workers = raw_frame_workers
                else:
                    opt_for_task.workers = max(1, int(opt.workers))
                log(
                    f"[FRAME START] #{zip_i:03d}/{total_zips:03d} {name} "
                    f"[source={src_label}, frame_workers={opt_for_task.workers}]"
                )
                
                future = frame_executor.submit(
                    _render_frame_task,
                    snapshot, zip_i, total_zips, frame_no, frame_png,
                    debug_snapshot_path, run_dir, frames_dir, opt_for_task, None, stage_path
                )
                start_time = time.time()
                with frame_lock:
                    in_flight_frames[future] = (snapshot, zip_i, start_time, src_label, stage_path)
                return future

            def _source_label(snapshot: SnapshotInput) -> str:
                return "cache" if is_cache_file(snapshot.path) else "raw"

            def _inflight_counts() -> Tuple[int, int]:
                with frame_lock:
                    vals = list(in_flight_frames.values())
                active = sum(1 for (_s, _zi, _st, _src, _stage) in vals)
                raw_active = sum(1 for (_s, _zi, _st, src, _stage) in vals if str(src).lower() == "raw")
                return active, raw_active

            def _can_submit_snapshot(snapshot: SnapshotInput) -> bool:
                _active, raw_active = _inflight_counts()
                if _source_label(snapshot) == "raw" and raw_active >= RAW_INFLIGHT_LIMIT:
                    return False
                return True

            def _try_submit_more() -> None:
                """Submit more work without exceeding concurrency or raw inflight cap.

                We rotate through the queue so cache-backed snapshots can start even if
                a raw snapshot at the front is temporarily blocked by RAW_INFLIGHT_LIMIT.
                """
                if not pending_zips_queue:
                    return
                attempts = len(pending_zips_queue)
                while attempts > 0:
                    active, _raw_active = _inflight_counts()
                    if active >= current_concurrency:
                        break
                    snapshot, zip_i = pending_zips_queue.popleft()
                    if _can_submit_snapshot(snapshot):
                        submitted_futures.append(submit_frame(snapshot, zip_i))
                    else:
                        pending_zips_queue.append((snapshot, zip_i))
                    attempts -= 1

            def trim_to_target_concurrency() -> None:
                # Cancel not-yet-running futures beyond current target and re-queue them.
                with frame_lock:
                    futures_snapshot = list(in_flight_frames.items())
                active_count = sum(1 for fut, _ in futures_snapshot if not fut.done())
                to_trim = max(0, active_count - current_concurrency)
                if to_trim <= 0:
                    return
                for fut, (snapshot, zip_i, _start, _src, _stage) in reversed(futures_snapshot):
                    if to_trim <= 0:
                        break
                    if fut.done() or fut.running():
                        continue
                    if fut.cancel():
                        with frame_lock:
                            in_flight_frames.pop(fut, None)
                        try:
                            submitted_futures.remove(fut)
                        except ValueError:
                            pass
                        pending_zips_queue.appendleft((snapshot, zip_i))
                        to_trim -= 1

            # Remove hard concurrency cap: allow as many as CPU and RAM allow
            submitted_futures = []
            displayed_eta_str: str = "--"
            _try_submit_more()

            # Process as frames complete and dynamically submit more
            last_block_update = time.time()
            while submitted_futures or pending_zips_queue:
                if cancel_event.is_set():
                    log("-" * 60)
                    stop_mode = str((stop_control or {}).get("mode", "partial_gif")).strip().lower()
                    if stop_mode == "immediate":
                        log("Stop requested (immediate). Ending render now; GIF build will be skipped.")
                    else:
                        log("Stop requested (partial GIF). Ending render and building GIF from completed frames.")
                    break

                # Check CPU usage every N seconds and adjust concurrency
                now = time.time()
                if has_psutil and (now - last_cpu_check) > cpu_check_interval:
                    try:
                        cpu_pct = psutil.cpu_percent(interval=0.2)
                        cpu_samples.append(float(cpu_pct))
                        avg_cpu = (sum(cpu_samples) / len(cpu_samples)) if cpu_samples else float(cpu_pct)
                        if (now - last_concurrency_change) >= concurrency_cooldown:
                            # Aim for a responsive-but-busy target around 75-85%.
                            if avg_cpu < target_cpu_low and current_concurrency < max_concurrency:
                                step = 2 if avg_cpu < max(20, target_cpu_low - 15) else 1
                                new_conc = min(max_concurrency, current_concurrency + step)
                                if new_conc != current_concurrency:
                                    current_concurrency = new_conc
                                    last_concurrency_change = now
                                    log(f"CPU {avg_cpu:.0f}% avg (low) → increasing concurrent frames to {current_concurrency}")
                            elif avg_cpu > target_cpu_high and current_concurrency > min_concurrency:
                                step = 2 if avg_cpu > min(99, target_cpu_high + 10) else 1
                                new_conc = max(min_concurrency, current_concurrency - step)
                                if new_conc != current_concurrency:
                                    current_concurrency = new_conc
                                    last_concurrency_change = now
                                    log(f"CPU {avg_cpu:.0f}% avg (high) → decreasing concurrent frames to {current_concurrency}")
                                    trim_to_target_concurrency()
                        last_cpu_check = now
                    except Exception:
                        pass

                # Check for stuck frames every stuck_frame_check_interval
                if (now - last_stuck_check) > stuck_frame_check_interval:
                    stuck_frames = []
                    with frame_lock:
                        for future, (snapshot_obj, zip_i, start_time, src_label, stage_path) in in_flight_frames.items():
                            if future.done():
                                continue
                            elapsed = now - start_time
                            if elapsed > STUCK_FRAME_THRESHOLD:
                                nm = getattr(snapshot_obj, "display_name", str(snapshot_obj))
                                stuck_frames.append((nm, zip_i, elapsed, src_label, stage_path))
                    with frame_lock:
                        running_snapshot = [v for f, v in in_flight_frames.items() if not f.done()]
                    if running_snapshot:
                        oldest = max(now - float(s) for (_snap, _zi, s, _src, _stage) in running_snapshot)
                        has_raw_inflight = any((str(src).lower() == "raw") for (_snap, _zi, _s, src, _stage) in running_snapshot)
                        status_line = f"{len(running_snapshot)} in-flight | {len(pending_zips_queue)} queued"
                        stage_labels = sorted({
                            _read_last_stage(stage_path)
                            for (_snap, _zi, _s, _src, stage_path) in running_snapshot
                        })
                        log(f"[HEARTBEAT] {status_line} | oldest frame age={oldest:.0f}s | stages={', '.join(stage_labels)}")

                        if zip_times:
                            # Use max observed frame time (not mean) so one fast cached frame
                            # does not under-estimate timeout for slow raw frames.
                            ref_frame_s = max(1.0, float(max(zip_times)))
                            if has_raw_inflight:
                                # Raw overworld frames can be vastly slower than cache-backed frames.
                                # Prefer observed raw times; otherwise use a conservative baseline.
                                if raw_zip_times:
                                    ref_raw_s = max(1.0, float(max(raw_zip_times)))
                                    stall_threshold_now = max(45 * 60, min(3 * 60 * 60, int(3.0 * ref_raw_s)))
                                else:
                                    # No prior raw timing yet — allow up to 3 hours before declaring stall.
                                    stall_threshold_now = 3 * 60 * 60
                            else:
                                stall_threshold_now = max(4 * 60, min(12 * 60, int(4.0 * ref_frame_s)))
                        else:
                            stall_threshold_now = NO_COMPLETION_STALL_THRESHOLD

                        no_completion_age = now - last_completion_t
                        # If raw in-flight stage files are still being updated recently,
                        # treat that as forward progress and defer stall handling.
                        # Use a 300-second window to tolerate slow-patch chunk scans that
                        # can go quiet for 100-120 s between updates.
                        if has_raw_inflight:
                            recent_raw_progress = False
                            for (_snap, _zi, _s, src, stage_path) in running_snapshot:
                                if str(src).lower() != "raw":
                                    continue
                                age = _stage_age_seconds(stage_path)
                                if age is not None and age <= 300.0:
                                    recent_raw_progress = True
                                    break
                            if recent_raw_progress:
                                last_stuck_check = now
                                continue
                        if no_completion_age > stall_threshold_now:
                            force_early_shutdown = True
                            salvaged_now = 0
                            with frame_lock:
                                staged_retry: List[Tuple[SnapshotInput, int, str]] = []
                                for _fut, (s, zi, _st, src, _stage) in in_flight_frames.items():
                                    if s is None:
                                        continue
                                    if _fut.done():
                                        continue
                                    fno = frame_no_by_path.get(s.path, zi)
                                    fpng = frame_png_by_path.get(s.path, os.path.join(frames_dir, f"frame_{zi:04d}.png"))
                                    try:
                                        if os.path.isfile(fpng) and os.path.getsize(fpng) > 0:
                                            rendered_frames.append((fno, fpng, None))
                                            blocks_rendered += frame_block_counts.get(s.path, 1)
                                            salvaged_now += 1
                                            log(f"[STALL RECOVER] Using already-written frame: {os.path.basename(fpng)}")
                                            continue
                                    except Exception:
                                        pass
                                    staged_retry.append((s, zi, src))
                                stalled_retry_items = staged_retry
                            # Also preserve queued work so it can be retried after the stalled worker is torn down.
                            queued_retry_items = [
                                (s, zi, _source_label(s)) for (s, zi) in list(pending_zips_queue)
                            ]
                            log(
                                "[STALL] No frame has completed for "
                                f"{no_completion_age:.0f}s (threshold={stall_threshold_now}s) "
                                f"with {len(running_snapshot)} in-flight. "
                                "Proceeding with completed frames and skipping stalled workers."
                            )
                            if salvaged_now > 0:
                                log(f"[STALL RECOVER] Salvaged {salvaged_now} in-flight frame(s) from existing PNG output.")
                            status(
                                "Frame workers stalled; proceeding with completed frames.",
                                "Building GIF from finished frames only."
                            )
                            break
                    if stuck_frames:
                        for name, zip_i, elapsed, src, stage_path in stuck_frames:
                            stage_name = _read_last_stage(stage_path)
                            log(f"WARNING: Frame {zip_i} ({name}, source={src}) has been running for {elapsed/3600:.2f} hours. Possible stuck worker.")
                            status(f"Frame {zip_i} appears stuck", f"{name} (source={src}) stage={stage_name} running {elapsed/3600:.2f}h")
                    last_stuck_check = now

                # Wait for next frame to complete with a short timeout so we can poll CPU and submit more
                try:
                    completed_future = next(as_completed(submitted_futures, timeout=0.5))
                    submitted_futures.remove(completed_future)
                    try:
                        success, bounds, name, error_reason, error_log_path = completed_future.result()
                        last_completion_t = time.time()
                        with frame_lock:
                            snapshot, zip_i, start_time, _src, _stage = in_flight_frames.pop(completed_future, (None, None, None, "unknown", ""))
                        if success:
                            if snapshot:
                                frame_no = frame_no_by_path.get(snapshot.path, zip_i)
                                frame_png = frame_png_by_path.get(snapshot.path, os.path.join(frames_dir, f"frame_{zip_i:04d}.png"))
                                rendered_frames.append((frame_no, frame_png, bounds))
                                elapsed = max(0.0, time.time() - float(start_time or time.time()))
                                zip_times.append(elapsed)
                                if str(_src).lower() == "raw":
                                    raw_zip_times.append(elapsed)
                                else:
                                    cache_zip_times.append(elapsed)
                                log(f"[FRAME DONE] #{zip_i:03d}/{total_zips:03d} {name} in {elapsed:.1f}s")
                                if (not opt.limit_enabled) and (not dynamic_area_refined) and bounds is not None:
                                    # Keep per-frame estimates stable to avoid large progress/ETA jumps.
                                    dynamic_area_refined = True
                                # Add blocks for this frame
                                blocks_rendered += frame_block_counts.get(snapshot.path, 1)
                                last_error_reason = ""
                                repeated_error_count = 0
                        else:
                            if snapshot:
                                reason = error_reason or "Render failed"
                                skipped.append((name, reason, error_log_path or ""))
                                log(f"[FRAME FAILED] {name}: {reason}")
                                if error_log_path:
                                    log(f"  error log: {error_log_path}")
                                if not rendered_frames:
                                    if reason == last_error_reason:
                                        repeated_error_count += 1
                                    else:
                                        last_error_reason = reason
                                        repeated_error_count = 1
                                    if repeated_error_count >= 8:
                                        repeated_error_abort = True
                                        log("Aborting early: repeated identical frame failures detected before any successful frame.")
                                        status("Aborted early due to repeated identical frame failures.", reason)
                                        cancel_event.set()
                                        pending_zips_queue.clear()
                                        break
                    except Exception as e:
                        log(f"Error collecting frame result: {e}")
                        with frame_lock:
                            crashed_info = in_flight_frames.pop(completed_future, None)
                        # Count the crashed frame as skipped so it is not silently dropped.
                        # Without this, a BrokenProcessPool or other exception leaves the
                        # frame neither rendered nor skipped, and the stall detector must
                        # wait 8+ minutes before recovering.
                        if crashed_info is not None:
                            crashed_snapshot, crashed_zip_i, _, crashed_src, _ = crashed_info
                            crashed_name = getattr(crashed_snapshot, "display_name", str(crashed_snapshot)) if crashed_snapshot else "unknown"
                            skipped.append((crashed_name, f"Worker exception: {type(e).__name__}: {e}", ""))
                            log(f"[FRAME CRASH] #{crashed_zip_i} {crashed_name}: {type(e).__name__}: {e}")
                except TimeoutError:
                    # No frames completed in timeout; keep looping to check CPU and submit more
                    pass

                # Try to submit more frames if we're below target concurrency
                if not cancel_event.is_set():
                    _try_submit_more()

                # Update overall progress (block-based)
                rendered_count = len(rendered_frames)
                finished_count = rendered_count + len(skipped)
                with frame_lock:
                    in_flight = sum(1 for f in in_flight_frames.keys() if not f.done())
                pending = len(pending_zips_queue)
                # Live block counter update
                now = time.time()
                if now - last_block_update > 1.0:
                    msgq.put(("live_blocks", blocks_rendered))
                    last_block_update = now

                now = time.time()
                avg_frame_seconds = (sum(zip_times) / len(zip_times)) if zip_times else None
                with frame_lock:
                    inflight_snapshot = list(in_flight_frames.values())
                inflight_raw = sum(1 for _snapshot, _zip_i, _start, src, _stage in inflight_snapshot if str(src).lower() == "raw")
                inflight_cache = max(0, len(inflight_snapshot) - inflight_raw)
                pending_raw = sum(1 for snap, _zip_i in pending_zips_queue if _source_label(snap) == "raw")
                pending_cache = max(0, len(pending_zips_queue) - pending_raw)
                inflight_frame_progress = 0.0
                raw_live_remaining_seconds = 0.0
                raw_live_chunk_rates: List[float] = []
                pending_raw_chunk_est = 0.0
                for _snapshot, _zip_i, start_time, _src, _stage in inflight_snapshot:
                    elapsed = max(0.0, now - float(start_time or now))
                    denom = float(avg_frame_seconds) if (avg_frame_seconds and avg_frame_seconds > 1.0) else startup_frame_eta_guess_s
                    inflight_frame_progress += min(0.95, max(0.01, elapsed / max(1.0, denom)))

                    if str(_src).lower() == "raw":
                        parsed = _parse_stage_progress(_read_last_stage(_stage))
                        if parsed is not None:
                            done_chunks, total_chunks = parsed
                            if done_chunks > 100 and elapsed > 10.0:
                                rate = float(done_chunks) / max(1.0, elapsed)
                                if rate > 0.01:
                                    raw_live_chunk_rates.append(rate)
                                    remaining_chunks = max(0, total_chunks - done_chunks)
                                    raw_live_remaining_seconds += float(remaining_chunks) / rate

                for snap, _zip_i in pending_zips_queue:
                    if _source_label(snap) != "raw":
                        continue
                    pending_raw_chunk_est += max(1.0, float(frame_block_counts.get(snap.path, 256)) / 256.0)

                effective_done_frames = min(float(total_zips), float(rendered_count) + inflight_frame_progress)
                frame_frac_done = effective_done_frames / max(1.0, float(total_zips))

                # Block-based completion: completed frames only (no in-flight inflation).
                blocks_done = max(0.0, float(blocks_rendered))
                blocks_total = max(1.0, float(total_blocks))
                block_frac_done = min(1.0, blocks_done / blocks_total)
                block_percent = int(round(block_frac_done * 100.0))
                eta_str = "--"
                eta_est = None
                
                # Improved ETA logic with source-aware timing so fast cache frames do not
                # unrealistically collapse ETA while long raw frames are still pending.
                if finished_count >= 1:
                    cache_avg_seconds = (sum(cache_zip_times) / len(cache_zip_times)) if cache_zip_times else None
                    raw_avg_seconds = (sum(raw_zip_times) / len(raw_zip_times)) if raw_zip_times else None

                    if cache_avg_seconds is None:
                        # No completed cache frame yet; use a conservative but responsive default.
                        cache_avg_seconds = min(60.0, float(avg_frame_seconds or startup_frame_eta_guess_s))

                    if raw_avg_seconds is None:
                        raw_elapsed_now = [
                            max(0.0, now - float(start_time or now))
                            for _snapshot, _zip_i, start_time, src, _stage in inflight_snapshot
                            if str(src).lower() == "raw"
                        ]
                        if raw_live_chunk_rates:
                            med_rate = sorted(raw_live_chunk_rates)[len(raw_live_chunk_rates) // 2]
                            if med_rate > 0.01:
                                raw_live_remaining_seconds += pending_raw_chunk_est / med_rate
                                raw_avg_seconds = max(60.0, raw_live_remaining_seconds / max(1.0, float(max(1, inflight_raw + pending_raw))))
                            else:
                                raw_avg_seconds = startup_frame_eta_guess_s * 4.0
                        elif raw_elapsed_now:
                            # Bootstrap raw ETA from current in-flight raw duration, biased high
                            # until at least one raw frame completes.
                            raw_avg_seconds = max(startup_frame_eta_guess_s * 4.0, max(raw_elapsed_now) * 1.5)
                        elif (pending_raw + inflight_raw) > 0:
                            raw_avg_seconds = startup_frame_eta_guess_s * 4.0
                        else:
                            raw_avg_seconds = cache_avg_seconds

                    remaining_frame_seconds = (
                        float(pending_cache + inflight_cache) * float(cache_avg_seconds)
                        + float(pending_raw + inflight_raw) * float(raw_avg_seconds)
                    )
                    effective_parallel = max(1.0, float(in_flight))
                    eta_est = remaining_frame_seconds / effective_parallel
                    
                    # Use appropriate smoother based on progression
                    if finished_count <= 2:
                        # During startup (first 1-2 frames), use quick-response smoother
                        first_frame_eta_smoother.add(eta_est)
                        smooth_eta = first_frame_eta_smoother.value()
                    else:
                        # After 2+ frames completed, blend into steady-state smoother for stability
                        steady_state_eta_smoother.add(eta_est)
                        smooth_eta = steady_state_eta_smoother.value()
                    
                    if smooth_eta is not None:
                        overall_eta_smoother.add(smooth_eta)
                        eta_val = overall_eta_smoother.value()
                    else:
                        eta_val = eta_est
                elif block_frac_done > 0.005:
                    elapsed_total = max(0.001, now - job_start)
                    eta_est = estimate_remaining_seconds(elapsed_total, block_frac_done)
                    if eta_est is not None:
                        overall_eta_smoother.add(eta_est)
                        eta_val = overall_eta_smoother.value()
                    else:
                        eta_val = None
                else:
                    eta_val = None

                if eta_val is not None:
                    displayed_eta_str = fmt_seconds(eta_val)
                status(
                    f"Frames rendered: {len(rendered_frames)}/{total_zips} | Block progress: {block_percent}% complete | ETA: {displayed_eta_str}",
                    f"{in_flight} in-flight | {pending} queued"
                )

                progress(block_frac_done * RENDER_WEIGHT)
                if repeated_error_abort:
                    break
        finally:
            _unregister_process_pool(frame_executor)
            if cancel_event.is_set() or force_early_shutdown:
                try:
                    frame_executor.shutdown(wait=False, cancel_futures=True)
                except Exception:
                    pass
                _force_terminate_pool(frame_executor, "main")
            else:
                try:
                    frame_executor.shutdown(wait=True)
                except Exception:
                    pass

        # If we bailed out due stalled workers, retry those specific frames serially
        # in the parent process so we can recover from child-process deadlocks.
        retry_items: List[Tuple[SnapshotInput, int, str]] = []
        if force_early_shutdown:
            retry_items.extend(stalled_retry_items)
            retry_items.extend(queued_retry_items)

        if retry_items and (not cancel_event.is_set()):
            log("-" * 60)
            log(f"[STALL] Retrying {len(retry_items)} frame(s) sequentially after stall...")
            for snapshot, zip_i, src_label in retry_items:
                name = snapshot.display_name
                frame_no = frame_no_by_path.get(snapshot.path, zip_i)
                frame_png = frame_png_by_path.get(snapshot.path, os.path.join(frames_dir, f"frame_{zip_i:04d}.png"))
                debug_snapshot_path = os.path.join(run_dir, f"debug_block_ids_{zip_i:03d}_{safe_filename(name)}.txt")
                stage_path = _stage_file_path(zip_i, name)
                log(f"[FRAME RETRY] #{zip_i:03d}/{total_zips:03d} {name} [source={src_label}]")

                # Hard timeout per retry so a single stuck frame cannot hang the whole run.
                # Raw frames restart the full chunk scan from scratch, so allow 3 hours.
                retry_timeout_s = (3 * 60 * 60) if str(src_label).lower() == "raw" else (10 * 60)
                retry_executor = ProcessPoolExecutor(max_workers=1)
                _register_process_pool(retry_executor)
                try:
                    retry_opt = clone_render_options(opt)
                    if str(src_label).lower() == "raw":
                        retry_opt.workers = raw_frame_workers
                    else:
                        retry_opt.workers = max(1, int(opt.workers))
                    retry_fut = retry_executor.submit(
                        _render_frame_task,
                        snapshot,
                        zip_i,
                        total_zips,
                        frame_no,
                        frame_png,
                        debug_snapshot_path,
                        run_dir,
                        frames_dir,
                        retry_opt,
                        None,
                        stage_path,
                    )
                    success, bounds, _nm, error_reason, error_log_path = retry_fut.result(timeout=retry_timeout_s)
                except TimeoutError:
                    # If the frame PNG exists, treat as success with unknown bounds.
                    # This covers cases where rendering finished but worker cleanup hung.
                    if os.path.isfile(frame_png) and os.path.getsize(frame_png) > 0:
                        success, bounds = True, None
                        error_reason = ""
                        error_log_path = ""
                        log(f"[FRAME RETRY SALVAGE] Timeout but frame file exists, accepting output: {os.path.basename(frame_png)}")
                    else:
                        success, bounds = False, None
                        stage_name = _read_last_stage(stage_path)
                        error_reason = f"Retry timed out after {retry_timeout_s}s (last stage: {stage_name})"
                        error_log_path = ""
                except Exception as _retry_exc:
                    success, bounds = False, None
                    error_reason = f"Retry worker error: {type(_retry_exc).__name__}: {_retry_exc}"
                    error_log_path = ""
                finally:
                    _unregister_process_pool(retry_executor)
                    try:
                        retry_executor.shutdown(wait=False, cancel_futures=True)
                    except Exception:
                        pass
                    _force_terminate_pool(retry_executor, f"retry #{zip_i}")

                if success:
                    rendered_frames.append((frame_no, frame_png, bounds))
                    blocks_rendered += frame_block_counts.get(snapshot.path, 1)
                    log(f"[FRAME RETRY DONE] #{zip_i:03d}/{total_zips:03d} {name}")
                else:
                    reason = error_reason or "Retry failed"
                    skipped.append((name, reason, error_log_path or ""))
                    log(f"[FRAME RETRY FAILED] {name}: {reason}")
                    if error_log_path:
                        log(f"  error log: {error_log_path}")

        run_elapsed = max(0.0, time.time() - job_start)
        _record_auto_tuner_result(
            str(strategy.get("machine_key", "unknown")),
            {
                "frame_workers": int(opt.workers),
                "initial_concurrency": int(current_concurrency),
                "max_concurrency": int(max_concurrency),
            },
            rendered=len(rendered_frames),
            elapsed_s=run_elapsed,
            failed=len(skipped),
        )

        stop_mode = str((stop_control or {}).get("mode", "partial_gif")).strip().lower()

        if cancel_event.is_set() and stop_mode == "immediate":
            log("-" * 60)
            log("Stopped immediately by user. Skipping GIF build.")
            run_log.close()
            msgq.put(("done", {
                "run_dir": run_dir,
                "frames_dir": frames_dir,
                "gif": "",
                "skipped": len(skipped),
                "skipped_report": None,
                "unknown_blocks_json": None,
                "cancelled": True,
                "stopped_mode": "immediate",
            }))
            return

        if not rendered_frames:
            if cancel_event.is_set():
                log("-" * 60)
                log("Stopped by user before any frame completed.")
                run_log.close()
                msgq.put(("done", {
                    "run_dir": run_dir,
                    "frames_dir": frames_dir,
                    "gif": "",
                    "skipped": len(skipped),
                    "skipped_report": None,
                    "unknown_blocks_json": None,
                    "cancelled": True,
                    "stopped_mode": "partial_gif",
                }))
                return
            # Collect Y-range info from all cache files to give an actionable advisory.
            y_range_advisory = ""
            try:
                seen_ranges: set = set()
                for snap in snapshots:
                    cache_src = snap.cache_path or (snap.path if is_cache_file(snap.path) else None)
                    if cache_src and os.path.isfile(cache_src):
                        try:
                            h = read_cache_header(cache_src)
                            y_lo = int(h.get("y_min", 0))
                            y_hi = int(h.get("y_max", 0))
                            mode = str(h.get("cache_mode", CACHE_MODE_SURFACE))
                            seen_ranges.add((y_lo, y_hi, mode))
                        except Exception:
                            pass
                if seen_ranges:
                    lines = []
                    for y_lo, y_hi, mode in sorted(seen_ranges):
                        mode_label = "surface" if mode == CACHE_MODE_SURFACE else "all-blocks" if mode == CACHE_MODE_ALL_BLOCKS else mode
                        lines.append(f"  Y=[{y_lo},{y_hi}] ({mode_label} cache)")
                    y_range_advisory = (
                        f"\n\nYour caches support:\n" + "\n".join(lines) +
                        f"\n\nYou requested Y=[{opt.y_min},{opt.y_max}]. "
                        "Adjust the Y range to match one of the above, or rebuild caches with 'Cache all blocks'."
                    )
            except Exception:
                pass
            msgq.put(("error",
                "All backups failed to render; no frames were produced." +
                y_range_advisory +
                "\n\nCheck skipped_backups.txt and run.log in the output folder for details."
            ))
            run_log.close()
            return

        if cancel_event.is_set():
            log("Stop requested: building GIF from completed frames.")
        status("Building animated GIF…", "")
        log("-" * 60)
        log(f"Building GIF from {len(rendered_frames)} frames…")
        progress(93.0)

        # Sort frames chronologically, then align to a common geographic canvas.
        rendered_frames_sorted = sorted(rendered_frames, key=lambda t: t[0])
        aligned_dir = os.path.join(frames_dir, "aligned")
        gif_frame_paths = align_and_composite_frames(rendered_frames_sorted, aligned_dir, log)

        world_name = ""
        try:
            world_name = os.path.splitext(os.path.basename(snapshots[0].display_name))[0]
        except Exception:
            world_name = ""
        base = getattr(opt, "output_name", "") or (safe_filename(world_name) if world_name else "timelapse")
        if not base.lower().endswith("_wmtt4mc"):
            base = base + "_wmtt4mc"
        out_gif = os.path.join(run_dir, base + ".gif")
        build_gif(gif_frame_paths, out_gif, seconds_per_frame=seconds_per_frame, loop=0)

        progress(100.0)
        log(f"Saved GIF: {out_gif}")

        # Aggregate per-frame _unknowns.json files into a single run-level unknown_blocks.json
        unknown_blocks_json_path = None
        try:
            global_unknowns: Counter = Counter()
            for _frame_no, frame_png, _bounds in rendered_frames:
                frame_json = os.path.splitext(frame_png)[0] + "_unknowns.json"
                if os.path.isfile(frame_json):
                    try:
                        with open(frame_json, "r", encoding="utf-8") as _jf:
                            data = json.load(_jf)
                        if isinstance(data, dict):
                            for blk, cnt in data.items():
                                global_unknowns[str(blk)] += int(cnt)
                    except Exception:
                        pass
            if global_unknowns:
                unknown_blocks_json_path = os.path.join(run_dir, "unknown_blocks.json")
                with open(unknown_blocks_json_path, "w", encoding="utf-8") as _jf:
                    json.dump(
                        {k: v for k, v in global_unknowns.most_common()},
                        _jf, indent=2
                    )
                log("-" * 60)
                log(f"[UNKNOWNS] {len(global_unknowns)} unknown block type(s) rendered gray.")
                log(f"  Open 'unknown_blocks.json' in the Palette Editor tab to assign colors:")
                log(f"  {unknown_blocks_json_path}")
        except Exception as _unk_exc:
            log(f"[UNKNOWNS] Warning: failed to write unknown_blocks.json: {_unk_exc}")

        skipped_report = None
        if skipped:
            skipped_report = os.path.join(run_dir, "skipped_backups.txt")
            with open(skipped_report, "w", encoding="utf-8") as f:
                f.write("Skipped backups:\\n\\n")
                for nm, reason, detail_path in skipped:
                    f.write(f"- {nm}\\n  {reason}\\n\\n")
                    if detail_path:
                        f.write(f"  Details: {detail_path}\\n\\n")
            log("-" * 60)
            log(f"Wrote skip report: {skipped_report}")
        
        run_log.close()

        if skipped:
            msgq.put(("error",
                f"Timelapse failed: {len(skipped)} of {total_zips} frame(s) failed to render. "
                f"A partial GIF was created at: {out_gif}\\n"
                f"See skip report for details: {skipped_report}"
            ))
            return

        msgq.put(("done", {
            "run_dir": run_dir,
            "frames_dir": frames_dir,
            "gif": out_gif,
            "skipped": len(skipped),
            "skipped_report": skipped_report,
            "unknown_blocks_json": unknown_blocks_json_path,
            "cancelled": False,
        }))

    except Exception:
        msgq.put(("error", traceback.format_exc()))


def _render_frame_task(
    snapshot: SnapshotInput, zip_i: int, total_zips: int,
    frame_no: int, frame_png: str,
    debug_snapshot_path: str, run_dir: str, frames_dir: str,
    opt: RenderOptions,
    cancel_event: Any = None,
    stage_path: Optional[str] = None,
) -> Tuple[bool, Optional[Tuple], str, str, str]:
    """
    Render a single frame in a worker thread. Called by concurrent executor.
    Returns (success, bounds_or_none, name, error_reason, error_log_path) for result bookkeeping.
    """
    t0 = time.time()
    name = snapshot.display_name
    _last_stage_progress_emit = 0.0

    def _write_stage(stage: str) -> None:
        if not stage_path:
            return
        try:
            with open(stage_path, "w", encoding="utf-8") as sf:
                sf.write(f"{time.time():.3f}\t{stage}\n")
        except Exception:
            pass

    def _progress_stage(done: int, total: int, _pct: float) -> None:
        nonlocal _last_stage_progress_emit
        if total <= 0:
            return
        now = time.time()
        # Throttle stage updates so we avoid excessive file writes.
        if done != total and (now - _last_stage_progress_emit) < 8.0:
            return
        _last_stage_progress_emit = now
        _write_stage(f"raw.chunk_scan.progress {int(done)}/{int(total)}")

    try:
        _write_stage("frame.start")
        header = (
            f"Snapshot: {name}\n"
            f"Source path: {snapshot.path}\n"
            f"Dimension: {opt.dimension}\n"
            f"Y range: [{opt.y_min},{opt.y_max}]\n"
            f"Target: {opt.target_preset}\n"
            f"Workers: {opt.workers}\n"
            f"Fast scan: {opt.fast_scan}\n"
            f"Aggressive mode: {opt.aggressive_mode}\n"
            f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"NOTE: Unknown IDs are rendered gray and listed in this file.\n"
        )
        opt_for_frame = clone_render_options(opt)
        opt_for_frame.workers = max(1, int(opt.workers))
        bounds = render_snapshot_input(
            snapshot,
            frame_png,
            opt_for_frame,
            log_cb=None,
            progress_cb=_progress_stage,
            cancel_event=cancel_event,
            debug_snapshot_path=debug_snapshot_path,
            debug_context_header=header,
            stage_cb=_write_stage,
        )
        if not os.path.isfile(frame_png):
            raise RuntimeError(f"Frame image was not written: {frame_png}")
        _write_stage("frame.done")
        return (True, bounds, name, "", "")

    except Exception as e:
        _write_stage(f"frame.error {type(e).__name__}")
        # Write error details to a global error log for debugging
        global_error_log = os.path.join(run_dir if 'run_dir' in locals() else '.', f"frame_global_error_{zip_i:03d}_{name}.log")
        with open(global_error_log, "w", encoding="utf-8") as errf:
            errf.write(f"Frame failed: {name}\n")
            errf.write(f"Output path: {frame_png}\n")
            if stage_path:
                try:
                    with open(stage_path, "r", encoding="utf-8") as sf:
                        errf.write(f"Last stage: {sf.read().strip()}\n")
                except Exception:
                    pass
            errf.write(f"Exception: {type(e).__name__}: {e}\n")
            errf.write(traceback.format_exc())
        return (False, None, name, f"{type(e).__name__}: {e}", global_error_log)


def _cache_matches_requested_settings(cache_path: str, source_path: str, cache_mode: str, dimension: str, y_min: int, y_max: int) -> bool:
    try:
        header = read_cache_header(cache_path)
    except Exception:
        return False
    try:
        sig = build_source_signature(source_path)
    except Exception:
        return False
    for key in ("source_kind", "source_name", "source_size", "source_mtime_ns"):
        if header.get(key) != sig.get(key):
            return False
    return (
        str(header.get("cache_mode", "")) == str(cache_mode)
        and str(header.get("dimension", "")) == str(dimension)
        and int(header.get("y_min", 0)) == int(y_min)
        and int(header.get("y_max", 0)) == int(y_max)
    )


def _cache_mismatch_reason(
    cache_path: str,
    source_path: str,
    cache_mode: str,
    dimension: str,
    y_min: int,
    y_max: int,
) -> Tuple[bool, str]:
    """Return (is_match, reason) for cache preflight diagnostics."""
    if not os.path.isfile(cache_path):
        return False, "missing"

    try:
        header = read_cache_header(cache_path)
    except Exception as exc:
        return False, f"header_error:{type(exc).__name__}"

    try:
        sig = build_source_signature(source_path)
    except Exception as exc:
        return False, f"source_sig_error:{type(exc).__name__}"

    for key in ("source_kind", "source_name", "source_size", "source_mtime_ns"):
        hv = header.get(key)
        sv = sig.get(key)
        if hv != sv:
            return False, f"source_mismatch:{key}"

    mode_h = str(header.get("cache_mode", ""))
    dim_h = str(header.get("dimension", ""))
    y0_h = int(header.get("y_min", 0))
    y1_h = int(header.get("y_max", 0))

    if mode_h != str(cache_mode):
        return False, f"mode_mismatch:{mode_h}->{cache_mode}"
    if dim_h != str(dimension):
        return False, f"dimension_mismatch:{dim_h}->{dimension}"
    if y0_h != int(y_min) or y1_h != int(y_max):
        return False, f"y_mismatch:[{y0_h},{y1_h}]->[{y_min},{y_max}]"

    return True, "match"


def cache_build_worker(
    snapshots: List[SnapshotInput],
    cache_mode: str,
    dimensions: "Union[str, List[str]]",
    y_min: int,
    y_max: int,
    msgq: "queue.Queue[tuple]",
    cancel_event: threading.Event,
):
    """Build caches for *snapshots* across one or more *dimensions*.

    Each dimension produces its own sidecar file, e.g.
    ``World_overworld.wmtt4mc`` and ``World_nether.wmtt4mc``.
    A single ``cache_done`` message is emitted when all dimensions finish.
    """
    # Normalise to list
    if isinstance(dimensions, str):
        dimensions = [dimensions]
    dimensions = [d for d in dimensions if d]
    if not dimensions:
        dimensions = ["minecraft:overworld"]

    n_snaps = len(snapshots)
    n_dims = len(dimensions)
    total_items = n_snaps * n_dims

    total_built = 0
    total_skipped = 0
    total_failed = 0

    def log(msg: str):
        msgq.put(("log", msg))

    def _fmt_eta(seconds: float) -> str:
        s = max(0, int(seconds))
        return f"{s // 3600:02d}:{(s % 3600) // 60:02d}:{s % 60:02d}"

    # Determine how many cache builds to run concurrently.
    # Each concurrent cache build spins up its own subprocess + extracts the world to a
    # temp folder.  The bottleneck is typically I/O (zip extraction + LevelDB reads), so
    # parallelism helps significantly.  Cap based on available RAM/cores.
    try:
        import psutil as _psutil
        _logical = int(os.cpu_count() or 1)
        _mem_gb = int(round(float(_psutil.virtual_memory().total) / (1024.0 ** 3)))
        # Rule of thumb: allow ~2 GB per concurrent build; also don't flood CPUs.
        _by_mem = max(1, min(8, _mem_gb // 2))
        _by_cpu = max(1, min(8, _logical // 2))
        cache_concurrency = max(1, min(_by_mem, _by_cpu))
    except Exception:
        _logical = int(os.cpu_count() or 1)
        cache_concurrency = max(1, min(4, _logical // 2))

    if total_items == 1:
        cache_concurrency = 1  # no point spinning up an executor for one item

    log(f"Cache build concurrency: {cache_concurrency} simultaneous build(s) for {total_items} item(s)")

    build_start = time.time()

    # Shared state guarded by a lock (multiple threads update these).
    _state_lock = threading.Lock()
    known_chunk_totals: List[int] = []
    completed_chunk_units = [0.0]           # list so closure can mutate
    items_finished = [0]                    # items fully done (built+skipped+failed)
    eta_last_display_t = [0.0]
    eta_last_text = ["ETA estimating..."]

    # Enumerate all (snapshot, dimension) work items up front.
    work_items: List[Tuple[int, Any, str]] = []  # (item_index, snapshot, dimension)
    for snap_idx, snapshot in enumerate(snapshots):
        for dim_idx, dimension in enumerate(dimensions):
            work_items.append((len(work_items) + 1, snapshot, dimension))

    def _build_one(item_index: int, snapshot: Any, dimension: str) -> Dict[str, Any]:
        """Build (or skip) a single cache entry.  Runs in a thread-pool thread."""
        if cancel_event.is_set():
            return {"status": "cancelled", "item_index": item_index}

        dim_label = dimension.replace("minecraft:", "").replace("the_", "")
        source_path = snapshot.raw_path or snapshot.path
        cache_path = sidecar_cache_path(source_path, dimension, cache_mode)
        display_name = snapshot.display_name
        item_label = f"{display_name} [{dim_label}]" if n_dims > 1 else display_name

        if os.path.isfile(cache_path) and _cache_matches_requested_settings(
            cache_path, source_path, cache_mode, dimension, y_min, y_max
        ):
            log(f"Skipping up-to-date cache: {item_label}")
            return {"status": "skipped", "item_index": item_index, "item_label": item_label}

        msgq.put(("status", (
            f"Building cache {item_index}/{total_items}: {item_label}",
            f"Mode: {cache_mode} | Y=[{y_min},{y_max}] | {cache_concurrency} parallel",
        )))

        # Per-item chunk progress callback — thread-safe.
        _local_seen_total: List[Optional[int]] = [None]
        _local_last_done: List[int] = [0]

        def chunk_progress(done: int, chunks_total: int, frac_done: float,
                           _item_idx=item_index, _display=item_label):
            _local_seen_total[0] = int(chunks_total)
            _local_last_done[0] = int(done)

            with _state_lock:
                known = list(known_chunk_totals)
                known.append(int(chunks_total))
                avg_chunks = float(sum(known)) / max(1, len(known))
                finished = int(items_finished[0])
                done_units = float(completed_chunk_units[0]) + float(done)
                remaining_items = max(0, total_items - finished - 1)
                est_total_units = done_units + (avg_chunks * remaining_items)
                remaining_units = max(0.0, est_total_units - done_units)
                overall_frac = min(1.0, (finished + frac_done) / max(1, total_items))

                now = time.time()
                elapsed = max(0.001, now - build_start)
                cum_rate = done_units / elapsed

                show_eta = cum_rate > 0.01 and done_units >= 50.0 and elapsed >= 10.0 and remaining_units > 0.0
                if show_eta and (now - eta_last_display_t[0]) >= 5.0:
                    eta_secs = remaining_units / max(0.01, cum_rate)
                    eta_last_text[0] = f"ETA ~{_fmt_eta(eta_secs)}"
                    eta_last_display_t[0] = now
                eta_text = eta_last_text[0]

            msgq.put(("progress", overall_frac * 100.0))
            msgq.put(("status", (
                f"Building cache {_item_idx}/{total_items}: {_display}",
                f"Overall {overall_frac * 100.0:5.1f}% | Chunks {done}/{chunks_total} "
                f"({frac_done * 100.0:.1f}%) | Rate {cum_rate:.1f} chunks/s | {eta_text}",
            )))

        try:
            build_snapshot_cache(
                snapshot, cache_mode, dimension, y_min, y_max,
                log_cb=log, progress_cb=chunk_progress, cancel_event=cancel_event,
            )
            return {
                "status": "built",
                "item_index": item_index,
                "item_label": item_label,
                "chunks_total": _local_seen_total[0],
            }
        except CancelledError:
            return {"status": "cancelled", "item_index": item_index}
        except RuntimeError as e:
            msg = str(e)
            if "no chunks found" in msg.lower():
                log(f"Skipping {item_label}: dimension has no chunks ({msg})")
                return {"status": "skipped_no_chunks", "item_index": item_index,
                        "item_label": item_label, "chunks_last": _local_last_done[0]}
            log(f"Cache build failed for {item_label}: {type(e).__name__}: {e}")
            log(traceback.format_exc())
            return {"status": "failed", "item_index": item_index,
                    "item_label": item_label, "chunks_last": _local_last_done[0]}
        except Exception as e:
            log(f"Cache build failed for {item_label}: {type(e).__name__}: {e}")
            log(traceback.format_exc())
            return {"status": "failed", "item_index": item_index,
                    "item_label": item_label, "chunks_last": _local_last_done[0]}

    try:
        with ThreadPoolExecutor(max_workers=cache_concurrency) as ex:
            futures = {
                ex.submit(_build_one, item_idx, snap, dim): (item_idx, snap, dim)
                for item_idx, snap, dim in work_items
            }
            for fut in as_completed(futures):
                if cancel_event.is_set():
                    break
                result = fut.result()
                status_val = result.get("status", "")
                with _state_lock:
                    items_finished[0] += 1
                    if status_val == "built":
                        total_built += 1
                        ct = result.get("chunks_total")
                        if ct is not None:
                            known_chunk_totals.append(int(ct))
                            completed_chunk_units[0] += float(ct)
                    elif status_val in ("skipped", "skipped_no_chunks"):
                        total_skipped += 1
                        cl = result.get("chunks_last", 0)
                        completed_chunk_units[0] += float(cl)
                    elif status_val == "failed":
                        total_failed += 1
                        cl = result.get("chunks_last", 0)
                        completed_chunk_units[0] += float(cl)
                    elif status_val == "cancelled":
                        pass

        if cancel_event.is_set():
            raise CancelledError("Cancelled during cache build.")

        msgq.put(("progress", 100.0))
        msgq.put(("cache_done", {
            "built": total_built,
            "skipped": total_skipped,
            "failed": total_failed,
            "total": total_items,
            "mode": cache_mode,
        }))
    except CancelledError:
        msgq.put(("cache_done", {
            "built": total_built,
            "skipped": total_skipped,
            "total": total_items,
            "mode": cache_mode,
            "cancelled": True,
        }))
    except Exception:
        msgq.put(("error", traceback.format_exc()))


def debug_one_chunk_worker(
    zip_path: str,
    out_dir: str,
    opt: RenderOptions,
    msgq: "queue.Queue[tuple]",
    cancel_event: threading.Event,
):
    try:
        dbg_id = time.strftime("%Y%m%d-%H%M%S")
        dbg_dir = os.path.join(out_dir, f"debug_chunk_{dbg_id}")
        os.makedirs(dbg_dir, exist_ok=True)

        log_path = os.path.join(dbg_dir, "debug.log")
        logf = open(log_path, "a", encoding="utf-8", buffering=1)

        def log(msg: str):
            msgq.put(("log", msg))
            try:
                logf.write(msg + "\\n")
            except Exception:
                pass

        def status(line1: str, line2: str = ""):
            msgq.put(("status", (line1, line2)))

        def progress(v: float):
            msgq.put(("progress", v))

        name = os.path.basename(zip_path)
        log(f"{APP_NAME} v{APP_VERSION} (build {APP_BUILD})")
        log(f"Debug folder: {dbg_dir}")
        log(f"Debug log: {log_path}")
        log(f"ZIP: {zip_path}")
        log(f"Options: dim={opt.dimension} y=[{opt.y_min},{opt.y_max}] skip_water={opt.skip_water} hillshade_mode={opt.hillshade_mode} fast_scan={opt.fast_scan}")
        log("-" * 60)

        if cancel_event and cancel_event.is_set():
            raise CancelledError("Cancelled before start.")

        status(f"Debug: extracting {name}…", "")
        progress(5.0)

        with tempfile.TemporaryDirectory() as tmp:
            extract_root, candidates = unzip_world_find_roots(zip_path, tmp)
            if not candidates:
                raise RuntimeError(f"No level.dat found after extraction. Extracted to: {extract_root}")

            log(f"Candidates (folders containing level.dat):")
            for c in candidates:
                log(f"  - {c}")

            chosen_root = None
            chosen_chunk = None
            dim_resolved = None

            for world_root in candidates:
                if cancel_event and cancel_event.is_set():
                    raise CancelledError("Cancelled during candidate scan.")
                try:
                    w = amulet.load_level(world_root)
                    try:
                        dim_resolved = resolve_dimension_id(w, opt.dimension)
                        coords = world_all_chunk_coords(w, dim_resolved)
                        if not coords:
                            continue
                        chosen_chunk = choose_debug_chunk(coords)
                        chosen_root = world_root
                        break
                    finally:
                        try:
                            w.close()
                        except Exception:
                            pass
                except Exception:
                    continue

            if not chosen_root or chosen_chunk is None:
                raise RuntimeError("Could not load any candidate world root for debug rendering.")

            cx, cz = chosen_chunk
            out_png = os.path.join(dbg_dir, f"debug_chunk_{cx}_{cz}.png")
            out_txt = os.path.join(dbg_dir, f"debug_chunk_{cx}_{cz}.txt")

            header = (
                f"Debug one-chunk render\\n"
                f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}\\n"
                f"ZIP: {zip_path}\\n"
                f"World root: {chosen_root}\\n"
                f"Dimension: {opt.dimension} (resolved: {dim_resolved})\\n"
                f"Chosen chunk: ({cx}, {cz})\\n"
                f"Y range: [{opt.y_min}, {opt.y_max}]\\n"
                f"Scale: 16 px per block (fixed for debug)\\n"
                f"Hillshade: {opt.hillshade_mode}\\n"
                f"Skip water: {opt.skip_water}\\n"
                f"Fast scan: {opt.fast_scan}\\n"
                f"NOTE: Unknown IDs are rendered gray and listed below.\\n"
            )

            status(f"Debug: rendering chunk ({cx},{cz})…", "")
            progress(30.0)

            opt2 = clone_render_options(opt)
            opt2.debug_block_samples = True

            render_one_chunk_png(
                chosen_root,
                out_png,
                opt2,
                chosen_chunk,
                out_txt,
                header,
                cancel_event=cancel_event,
                debug_scale=16,
            )

            progress(100.0)
            log("Debug complete.")
            log(f"PNG: {out_png}")
            log(f"Report: {out_txt}")
            logf.close()

            msgq.put(("done_debug", {
                "dbg_dir": dbg_dir,
                "png": out_png,
                "txt": out_txt,
                "log": log_path,
                "cancelled": False,
            }))

    except CancelledError:
        msgq.put(("done_debug", {
            "dbg_dir": None,
            "png": None,
            "txt": None,
            "log": None,
            "cancelled": True,
        }))
    except Exception:
        msgq.put(("error", traceback.format_exc()))



def preflight_report_worker(
    folder: str,
    out_dir: str,
    opt: RenderOptions,
    output_cache_mode: str,
    msgq: "queue.Queue[tuple]",
    cancel_event: threading.Event,
):
    """Generate a non-render preflight report describing discovered inputs and planned source usage."""
    try:
        run_id = time.strftime("%Y%m%d-%H%M%S")
        report_dir = os.path.join(out_dir, f"preflight_{run_id}")
        os.makedirs(report_dir, exist_ok=True)
        report_txt = os.path.join(report_dir, "preflight_report.txt")
        report_json = os.path.join(report_dir, "preflight_report.json")

        logf = open(report_txt, "a", encoding="utf-8", buffering=1)

        def log(msg: str):
            msgq.put(("log", msg))
            try:
                logf.write(str(msg).replace("\\r\\n", "\n").replace("\\n", "\n") + "\n")
            except Exception:
                pass

        def status(line1: str, line2: str = ""):
            msgq.put(("status", (line1, line2)))

        def progress(v: float):
            msgq.put(("progress", v))

        status("Running preflight report…", "Scanning backups and cache files.")
        progress(5.0)

        log(f"{APP_NAME} v{APP_VERSION} (build {APP_BUILD})")
        log(f"Preflight folder: {report_dir}")
        log(f"Input folder: {folder}")
        log(f"Output folder: {out_dir}")
        log(f"Dimension: {opt.dimension} | y=[{opt.y_min},{opt.y_max}]")
        log(f"Target: {opt.target_preset}")
        log(f"Output cache mode: {output_cache_mode or '(unspecified)'}")
        if opt.limit_enabled:
            log(f"Crop: enabled x=[{opt.x_min},{opt.x_max}] z=[{opt.z_min},{opt.z_max}]")
        else:
            log("Crop: disabled")
        log("-" * 60)

        if cancel_event and cancel_event.is_set():
            raise CancelledError("Cancelled before discovery.")

        diag_lines: List[str] = []

        def _diag_log(msg: str) -> None:
            text = str(msg)
            diag_lines.append(text)
            log(text)

        snapshots, diag = discover_with_diagnostics(
            folder,
            log_cb=_diag_log,
            dimension=opt.dimension,
        )
        progress(35.0)

        if cancel_event and cancel_event.is_set():
            raise CancelledError("Cancelled during discovery.")

        plan_items: List[Dict[str, Any]] = []
        log("[PREFLIGHT PLAN]")
        for i, snap in enumerate(snapshots, start=1):
            item: Dict[str, Any] = {
                "index": i,
                "display_name": snap.display_name,
                "kind": snap.kind,
                "path": snap.path,
                "raw_path": snap.raw_path,
                "cache_path": snap.cache_path,
                "warning": snap.warning,
            }
            if is_cache_file(snap.path):
                action = "use_cache"
                cache_name = os.path.basename(snap.path)
                mode = "?"
                y_lo = "?"
                y_hi = "?"
                try:
                    h = read_cache_header(snap.path)
                    mode = str(h.get("cache_mode", "?"))
                    y_lo = str(h.get("y_min", "?"))
                    y_hi = str(h.get("y_max", "?"))
                except Exception:
                    pass
                log(f"  {i:02d}. {snap.display_name} -> USE CACHE ({cache_name}) mode={mode} y=[{y_lo},{y_hi}]")
                item.update({
                    "planned_action": action,
                    "cache_header_mode": mode,
                    "cache_header_y_min": y_lo,
                    "cache_header_y_max": y_hi,
                })
            else:
                action = "use_raw"
                raw_src = snap.raw_path or snap.path
                surf_path = sidecar_cache_path(raw_src, opt.dimension, CACHE_MODE_SURFACE)
                allb_path = sidecar_cache_path(raw_src, opt.dimension, CACHE_MODE_ALL_BLOCKS)
                surf_ok, surf_reason = _cache_mismatch_reason(
                    surf_path, raw_src, CACHE_MODE_SURFACE, opt.dimension, opt.y_min, opt.y_max
                )
                allb_ok, allb_reason = _cache_mismatch_reason(
                    allb_path, raw_src, CACHE_MODE_ALL_BLOCKS, opt.dimension, opt.y_min, opt.y_max
                )
                log(
                    f"  {i:02d}. {snap.display_name} -> USE RAW ({os.path.basename(raw_src)}) "
                    f"cache_match(surface={str(bool(surf_ok)).lower()}:{surf_reason}, "
                    f"all_blocks={str(bool(allb_ok)).lower()}:{allb_reason})"
                )
                log(f"      expected_surface={surf_path}")
                log(f"      expected_all_blocks={allb_path}")
                item.update({
                    "planned_action": action,
                    "raw_source": raw_src,
                    "expected_surface_cache": surf_path,
                    "expected_all_blocks_cache": allb_path,
                    "surface_match": bool(surf_ok),
                    "surface_reason": surf_reason,
                    "all_blocks_match": bool(allb_ok),
                    "all_blocks_reason": allb_reason,
                })

            plan_items.append(item)

        cache_count = sum(1 for p in plan_items if p.get("planned_action") == "use_cache")
        raw_count = sum(1 for p in plan_items if p.get("planned_action") == "use_raw")

        log("[PREFLIGHT SUMMARY]")
        log(f"  Planned cache-backed items: {cache_count}")
        log(f"  Planned raw items: {raw_count}")
        log(f"  Total planned items: {len(plan_items)}")
        progress(80.0)

        report_obj: Dict[str, Any] = {
            "generated_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "app": {
                "name": APP_NAME,
                "version": APP_VERSION,
                "build": APP_BUILD,
            },
            "settings": {
                "input_folder": folder,
                "output_folder": out_dir,
                "dimension": opt.dimension,
                "y_min": opt.y_min,
                "y_max": opt.y_max,
                "target": opt.target_preset,
                "output_cache_mode": output_cache_mode,
                "crop_enabled": bool(opt.limit_enabled),
                "crop": {
                    "x_min": opt.x_min,
                    "x_max": opt.x_max,
                    "z_min": opt.z_min,
                    "z_max": opt.z_max,
                } if opt.limit_enabled else None,
            },
            "discovery": diag,
            "discovery_log_lines": diag_lines,
            "plan": plan_items,
            "summary": {
                "planned_cache_items": cache_count,
                "planned_raw_items": raw_count,
                "planned_total_items": len(plan_items),
            },
        }

        with open(report_json, "w", encoding="utf-8") as f:
            json.dump(report_obj, f, indent=2, ensure_ascii=False)

        progress(100.0)
        status("Preflight report complete.", "Review report files in output folder.")
        log(f"Saved preflight text report: {report_txt}")
        log(f"Saved preflight JSON report: {report_json}")

        logf.close()
        msgq.put(("done_preflight", {
            "report_dir": report_dir,
            "report_txt": report_txt,
            "report_json": report_json,
            "planned_total": len(plan_items),
            "planned_cache": cache_count,
            "planned_raw": raw_count,
        }))
    except CancelledError:
        msgq.put(("done_preflight", {
            "cancelled": True,
        }))
    except Exception:
        msgq.put(("error", traceback.format_exc()))


class App(tk.Tk):
    def _poll_live_blocks(self):
        # Poll for live block count updates
        try:
            cfg = _load_config()
            live_blocks = cfg.get("live_blocks", 0)
            self.status2_var.set(f"Blocks rendered: {live_blocks:,}")
        except Exception:
            pass
        self.after(1000, self._poll_live_blocks)

    def _heartbeat(self):
        # Heartbeat to ensure UI doesn't hang if worker thread is alive but not making progress
        if self.worker_thread and self.worker_thread.is_alive():
            # If busy but no progress, update status to show still working
            if self.busy_var.get():
                self._set_status(self.status1_var.get(), "Working... (no progress update yet)")
            self.after(5000, self._heartbeat)

    def validate_crop_area(self, x_min, x_max, z_min, z_max, limit_enabled, target_preset, parent=None):
        """
        Validates the crop rectangle. Returns True if valid, False if blocked.
        Shows error/warning dialogs as needed.
        """
        if not limit_enabled:
            return True
        x1, x2 = int(min(x_min, x_max)), int(max(x_min, x_max))
        z1, z2 = int(min(z_min, z_max)), int(max(z_min, z_max))
        width = abs(x2 - x1) + 1
        height = abs(z2 - z1) + 1
        area = width * height
        min_edge = min(width, height)
        # Hard error if any edge < 32
        if min_edge < 32:
            msg = f"Selected crop area is too small.\n\nWidth: {width:,} blocks\nHeight: {height:,} blocks\n\nMinimum allowed: 32x32 blocks."
            messagebox.showerror("Crop area too small", msg, parent=parent)
            return False
        # Warn if area is smaller than video resolution
        target = parse_target_preset(target_preset)
        if target:
            px_count = target[0] * target[1]
            if area < px_count:
                # Format numbers with commas
                width_f = f"{width:,}"
                height_f = f"{height:,}"
                area_f = f"{area:,}"
                px_count_f = f"{px_count:,}"
                # Recommend closest video resolution
                resolutions = [(1280, 720), (1920, 1080), (3840, 2160)]
                res_names = ["720p (1280x720)", "1080p (1920x1080)", "4K (3840x2160)"]
                area_diffs = [abs((w*h)-area) for (w,h) in resolutions]
                min_diff = min(area_diffs)
                if area < min(w*h for (w,h) in resolutions):
                    rec = 'original (no scaling)'
                else:
                    idx = area_diffs.index(min_diff)
                    rec = res_names[idx]
                msg = (
                    f"Selected crop area ({area_f} blocks) is smaller than the video resolution ({px_count_f} pixels).\n\n"
                    f"This may result in blurry or upscaled output.\n"
                    f"Recommended video resolution for this area: {rec}.\n\n"
                    f"Continue with your currently selected video resolution ({target_preset})?"
                )
                if not messagebox.askyesno("Crop area warning", msg, parent=parent):
                    return False
        return True

    def __init__(self):
        super().__init__()
        self._set_window_icon()
        self.title(f"{APP_ABBR} - {APP_NAME} v{APP_VERSION}")
        self.geometry("1000x720")
        self.minsize(900, 650)
        # Make scrollbars more visible (wider) across platforms
        self.style = ttk.Style(self)
        try:
            self.style.theme_use("clam")
        except Exception:
            pass
        self.style.configure("WMTT.Vertical.TScrollbar", width=16)
        self.style.configure("WMTT.Horizontal.TScrollbar", width=16)
        # Keep cache-mode combobox visuals explicit across themes:
        # white when editable (readonly), grey when locked (disabled).
        self.style.configure("CacheMode.TCombobox", fieldbackground="#ffffff", foreground="#000000")
        self.style.map(
            "CacheMode.TCombobox",
            fieldbackground=[("disabled", "#e0e0e0"), ("readonly", "#ffffff")],
            foreground=[("disabled", "#666666"), ("readonly", "#000000")],
        )
        # Timelapse dropdowns that are always active should remain white.
        self.style.configure("ActiveReadonly.TCombobox", fieldbackground="#ffffff", foreground="#000000")
        self.style.map(
            "ActiveReadonly.TCombobox",
            fieldbackground=[("readonly", "#ffffff")],
            foreground=[("readonly", "#000000")],
        )
        # --- shared state ---
        self.msgq: "queue.Queue[tuple]" = queue.Queue()
        self.cancel_event = threading.Event()
        self.worker_thread: Optional[threading.Thread] = None
        self.current_task: Optional[str] = None
        self.stop_control: Dict[str, str] = {"mode": "partial_gif"}
        self._close_pending = False
        self._close_deadline = 0.0
        self._paused_worker_pids: List[int] = []

        # --- Timelapse tab vars ---
        self.folder_var = tk.StringVar()
        self.out_var = tk.StringVar(value=os.path.join(os.path.expanduser("~"), "WMTT4MC_Output"))
        self.dimension_var = tk.StringVar(value="minecraft:overworld")

        # Crop/limit (must be initialized before use)
        self.limit_enabled_var = tk.BooleanVar(value=False)
        self.xmin_var = tk.IntVar(value=0)
        self.zmin_var = tk.IntVar(value=0)
        self.xmax_var = tk.IntVar(value=0)
        self.zmax_var = tk.IntVar(value=0)

        self.target_var = tk.StringVar(value="1080p (1920x1080)")
        self.custom_w_var = tk.IntVar(value=1920)
        self.custom_h_var = tk.IntVar(value=1080)

        # Primary timing control
        self.seconds_per_frame_var = tk.DoubleVar(value=1.0)
        # Advanced control (derived)
        self.fps_var = tk.DoubleVar(value=1.0)

        self.skip_water_var = tk.BooleanVar(value=False)
        self.hillshade_var = tk.StringVar(value="normal")
        self.use_editor_palette_var = tk.BooleanVar(value=False)
        self.ymin_var = tk.IntVar(value=0)
        self.ymax_var = tk.IntVar(value=320)

        # Output options
        self.output_name_var = tk.StringVar(value="")
        self.keep_frames_var = tk.BooleanVar(value=False)
        self.cache_mode_var = tk.StringVar(value=CACHE_MODE_SURFACE)
        self.cache_mode_combo = None
        self.cache_crop_note_var = tk.StringVar(value="")
        self._cache_mode_before_crop = CACHE_MODE_SURFACE
        self._cache_mode_before_target = CACHE_MODE_SURFACE
        self.cache_dim_overworld_var = tk.BooleanVar(value=True)
        self.cache_dim_nether_var = tk.BooleanVar(value=False)
        self.cache_dim_end_var = tk.BooleanVar(value=False)
        self.cache_dim_checks: list = []

        # Advanced options (hidden by toggle)
        self.advanced_open = tk.BooleanVar(value=False)
        self.debug_blocks_var = tk.BooleanVar(value=False)
        self.workers_var = tk.IntVar(value=3)
        self.fast_scan_var = tk.BooleanVar(value=False)
        self.aggressive_var = tk.BooleanVar(value=False)

        # --- Single-map tab vars ---
        self.single_zip_var = tk.StringVar()
        self.single_out_png_var = tk.StringVar(value=os.path.join(os.path.expanduser("~"), "wmtt4mc_map.png"))
        self.single_dimension_var = tk.StringVar(value="minecraft:overworld")
        self.single_target_var = tk.StringVar(value="Original (no scaling)")
        self.single_custom_w_var = tk.IntVar(value=3840)
        self.single_custom_h_var = tk.IntVar(value=2160)
        self.single_limit_enabled_var = tk.BooleanVar(value=False)
        self.single_xmin_var = tk.IntVar(value=0)
        self.single_zmin_var = tk.IntVar(value=0)
        self.single_xmax_var = tk.IntVar(value=0)
        self.single_zmax_var = tk.IntVar(value=0)
        self.single_skip_water_var = tk.BooleanVar(value=False)
        self.single_hillshade_var = tk.StringVar(value="normal")
        self.single_use_editor_palette_var = tk.BooleanVar(value=False)
        self.single_ymin_var = tk.IntVar(value=0)
        self.single_ymax_var = tk.IntVar(value=320)

        self._restore_ui_settings()

        # Status/progress
        self.status1_var = tk.StringVar(value="Ready.")
        self.status2_var = tk.StringVar(value="")
        self.progress_var = tk.DoubleVar(value=0.0)
        self.busy_var = tk.BooleanVar(value=False)

        self._build_ui()
        self._resize_to_fit()
        self.protocol("WM_DELETE_WINDOW", self.on_close)

        self._wire_timing_vars()

        self.after(100, self._poll_messages)
        self.after(400, self._maybe_show_first_run_help)

    # ---------- UI helpers ----------
    def _set_window_icon(self):
        """Set window/taskbar icon if assets are present."""
        try:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            ico_path = os.path.join(base_dir, "app_icon.ico")
            png_path = os.path.join(base_dir, "app_icon_1024.png")
            try:
                if os.path.exists(ico_path):
                    self.iconbitmap(ico_path)
            except Exception:
                pass
            try:
                if os.path.exists(png_path):
                    img = tk.PhotoImage(file=png_path)
                    self.iconphoto(True, img)
                    self._icon_img = img  # keep ref
            except Exception:
                pass
        except Exception:
            pass

    def _wire_timing_vars(self):
        def spf_changed(*_):
            try:
                spf = float(self.seconds_per_frame_var.get())
            except Exception:
                return
            if spf <= 0:
                spf = 0.1
                self.seconds_per_frame_var.set(spf)
            fps = 1.0 / spf
            # Avoid feedback loops by only setting when different
            try:
                if abs(float(self.fps_var.get()) - fps) > 1e-6:
                    self.fps_var.set(fps)
            except Exception:
                self.fps_var.set(fps)

        def fps_changed(*_):
            # Advanced override: if user edits FPS, update seconds per frame
            try:
                fps = float(self.fps_var.get())
            except Exception:
                return
            if fps <= 0:
                fps = 0.1
                self.fps_var.set(fps)
            spf = 1.0 / fps
            try:
                if abs(float(self.seconds_per_frame_var.get()) - spf) > 1e-6:
                    self.seconds_per_frame_var.set(spf)
            except Exception:
                self.seconds_per_frame_var.set(spf)

        self.seconds_per_frame_var.trace_add("write", spf_changed)
        self.fps_var.trace_add("write", fps_changed)
        spf_changed()

    def _restore_ui_settings(self):
        try:
            cfg = _load_config()

            last_in = cfg.get("last_input_dir")
            last_out = cfg.get("last_output_dir")
            if isinstance(last_in, str) and last_in and os.path.isdir(last_in):
                self.folder_var.set(last_in)
            if isinstance(last_out, str) and last_out and os.path.isdir(last_out):
                self.out_var.set(last_out)

            tl = cfg.get("timelapse", {}) if isinstance(cfg.get("timelapse", {}), dict) else {}
            crop_cfg = tl.get("crop", {}) if isinstance(tl.get("crop", {}), dict) else {}

            self.dimension_var.set(tl.get("dimension", self.dimension_var.get()))
            self.target_var.set(tl.get("target", self.target_var.get()))
            self.custom_w_var.set(int(tl.get("custom_w", self.custom_w_var.get())))
            self.custom_h_var.set(int(tl.get("custom_h", self.custom_h_var.get())))
            self.seconds_per_frame_var.set(float(tl.get("seconds_per_frame", self.seconds_per_frame_var.get())))
            self.skip_water_var.set(bool(tl.get("skip_water", self.skip_water_var.get())))
            self.hillshade_var.set(tl.get("hillshade_mode", self.hillshade_var.get()))
            self.use_editor_palette_var.set(bool(tl.get("use_editor_palette", self.use_editor_palette_var.get())))
            self.ymin_var.set(int(tl.get("y_min", self.ymin_var.get())))
            self.ymax_var.set(int(tl.get("y_max", self.ymax_var.get())))
            self.output_name_var.set(str(tl.get("output_name", self.output_name_var.get())))
            self.keep_frames_var.set(bool(tl.get("keep_frames", self.keep_frames_var.get())))
            self.cache_mode_var.set(str(tl.get("cache_mode", self.cache_mode_var.get())))
            self.cache_dim_overworld_var.set(bool(tl.get("cache_dim_overworld", self.cache_dim_overworld_var.get())))
            self.cache_dim_nether_var.set(bool(tl.get("cache_dim_nether", self.cache_dim_nether_var.get())))
            self.cache_dim_end_var.set(bool(tl.get("cache_dim_end", self.cache_dim_end_var.get())))
            # Performance tuning is automatic; keep legacy fields ignored for compatibility.
            self.workers_var.set(0)
            self.fast_scan_var.set(False)
            self.aggressive_var.set(False)
            self.debug_blocks_var.set(bool(tl.get("debug_blocks", self.debug_blocks_var.get())))

            self.limit_enabled_var.set(bool(crop_cfg.get("enabled", self.limit_enabled_var.get())))
            self.xmin_var.set(int(crop_cfg.get("x_min", self.xmin_var.get())))
            self.xmax_var.set(int(crop_cfg.get("x_max", self.xmax_var.get())))
            self.zmin_var.set(int(crop_cfg.get("z_min", self.zmin_var.get())))
            self.zmax_var.set(int(crop_cfg.get("z_max", self.zmax_var.get())))
        except Exception:
            pass

    def _persist_ui_settings(self):
        try:
            cfg = _load_config()
            cfg["last_input_dir"] = self.folder_var.get().strip()
            cfg["last_output_dir"] = self.out_var.get().strip()
            cfg["timelapse"] = {
                "dimension": self.dimension_var.get(),
                "target": self.target_var.get(),
                "custom_w": int(self.custom_w_var.get()),
                "custom_h": int(self.custom_h_var.get()),
                "seconds_per_frame": float(self.seconds_per_frame_var.get()),
                "skip_water": bool(self.skip_water_var.get()),
                "hillshade_mode": str(self.hillshade_var.get()),
                "y_min": int(self.ymin_var.get()),
                "y_max": int(self.ymax_var.get()),
                "output_name": self.output_name_var.get(),
                "keep_frames": bool(self.keep_frames_var.get()),
                "cache_mode": self.cache_mode_var.get(),
                "cache_dim_overworld": bool(self.cache_dim_overworld_var.get()),
                "cache_dim_nether": bool(self.cache_dim_nether_var.get()),
                "cache_dim_end": bool(self.cache_dim_end_var.get()),
                "auto_tune": True,
                "debug_blocks": bool(self.debug_blocks_var.get()),
                "use_editor_palette": bool(self.use_editor_palette_var.get()),
                "crop": {
                    "enabled": bool(self.limit_enabled_var.get()),
                    "x_min": int(self.xmin_var.get()),
                    "x_max": int(self.xmax_var.get()),
                    "z_min": int(self.zmin_var.get()),
                    "z_max": int(self.zmax_var.get()),
                },
            }
            _save_config(cfg)
        except Exception:
            pass

    
    def _resize_to_fit(self) -> None:
        """Resize the window to fit the currently visible content (advanced sections collapsed/expanded),
        capped to the user's screen. Keeps the window resizable; scrollbars handle smaller sizes.
        """
        try:
            self.update_idletasks()
            sw = self.winfo_screenwidth()
            sh = self.winfo_screenheight()

            # requested size of visible widgets only (pack_forget widgets are excluded)
            w = self.winfo_reqwidth() + 24
            h = self.winfo_reqheight() + 48

            # keep some breathing room and stay on-screen (Windows taskbar etc.)
            w = max(760, min(w, sw - 80))
            h = max(520, min(h, sh - 120))

            self.geometry(f"{w}x{h}")
        except Exception:
            # never crash the app just because geometry probing failed
            pass

    def _build_ui(self):
        pad = {"padx": 8, "pady": 6}

        # A slightly chunkier primary button helps discoverability.
        style = ttk.Style(self)
        try:
            style.configure("Go.TButton", padding=(14, 10), font=("Segoe UI", 11, "bold"))
        except Exception:
            pass

        root = ttk.Frame(self)
        root.pack(fill="both", expand=True)

        # Notebook (tabs)
        self.nb = ttk.Notebook(root)
        nb = self.nb
        nb.pack(fill="both", expand=True)

        self.tab_timelapse = ttk.Frame(nb)
        nb.add(self.tab_timelapse, text="Timelapse")

        sf1 = ScrollableFrame(self.tab_timelapse)
        sf1.pack(fill="both", expand=True)
        self._build_timelapse_tab(sf1.interior)

        self.tab_palette = ttk.Frame(nb)
        nb.add(self.tab_palette, text="Palette Editor")
        self._build_palette_tab(self.tab_palette)

        self.tab_help = ttk.Frame(nb)
        nb.add(self.tab_help, text="Help")
        self._build_help_tab(self.tab_help)

        # Footer: status + disclaimer
        footer = ttk.Frame(root)
        footer.pack(fill="x", side="bottom")

        ttk.Label(footer, text=DISCLAIMER_TEXT).pack(side="left", padx=8)
        ttk.Label(footer, text=f"{APP_ABBR} v{APP_VERSION} (build {APP_BUILD})").pack(side="right", padx=8)

    def _build_timelapse_tab(self, parent):
        pad = {"padx": 8, "pady": 6}

        top = ttk.Frame(parent)
        top.pack(fill="x", **pad)

        ttk.Label(top, text="Backups / caches folder:").grid(row=0, column=0, sticky="w", **pad)
        ttk.Entry(top, textvariable=self.folder_var, width=70).grid(row=0, column=1, sticky="we", **pad)
        ttk.Button(top, text="Browse…", command=self.pick_folder).grid(row=0, column=2, sticky="w", **pad)

        ttk.Label(top, text="Output folder:").grid(row=1, column=0, sticky="w", **pad)
        ttk.Entry(top, textvariable=self.out_var, width=70).grid(row=1, column=1, sticky="we", **pad)
        ttk.Button(top, text="Browse…", command=self.pick_output).grid(row=1, column=2, sticky="w", **pad)

        ttk.Button(top, text="Open output", command=self.open_output_folder).grid(row=1, column=3, sticky="w", **pad)
        top.columnconfigure(1, weight=1)

        # Options
        opts = ttk.LabelFrame(parent, text="Timelapse settings")
        opts.pack(fill="x", padx=10, pady=8)

        ttk.Label(opts, text="Dimension:").grid(row=0, column=0, sticky="w", **pad)
        ttk.Combobox(opts, textvariable=self.dimension_var,
                     values=["minecraft:overworld", "minecraft:the_nether", "minecraft:the_end"],
                     state="readonly", style="ActiveReadonly.TCombobox", width=22).grid(row=0, column=1, sticky="w", **pad)

        ttk.Label(opts, text="Video resolution:").grid(row=0, column=2, sticky="w", **pad)
        ttk.Combobox(opts, textvariable=self.target_var,
                     values=["720p (1280x720)", "1080p (1920x1080)", "4K (3840x2160)", "Original (no scaling)", "Custom…"],
                     state="readonly", style="ActiveReadonly.TCombobox", width=22).grid(row=0, column=3, sticky="w", **pad)

        ttk.Label(opts, text="Custom W×H:").grid(row=1, column=2, sticky="w", **pad)
        self.custom_wh_frame = ttk.Frame(opts)
        self.custom_wh_frame.grid(row=1, column=3, sticky="w", **pad)
        ttk.Entry(self.custom_wh_frame, textvariable=self.custom_w_var, width=8).pack(side="left")
        ttk.Label(self.custom_wh_frame, text="×").pack(side="left", padx=4)
        ttk.Entry(self.custom_wh_frame, textvariable=self.custom_h_var, width=8).pack(side="left")

        ttk.Label(opts, text="Seconds per frame:").grid(row=1, column=0, sticky="w", **pad)
        ttk.Entry(opts, textvariable=self.seconds_per_frame_var, width=10).grid(row=1, column=1, sticky="w", **pad)

        ttk.Label(opts, text="Y min / max:").grid(row=2, column=0, sticky="w", **pad)
        yrow = ttk.Frame(opts)
        yrow.grid(row=2, column=1, sticky="w", **pad)
        ttk.Entry(yrow, textvariable=self.ymin_var, width=6).pack(side="left")
        ttk.Label(yrow, text="to").pack(side="left", padx=4)
        ttk.Entry(yrow, textvariable=self.ymax_var, width=6).pack(side="left")

        ttk.Checkbutton(opts, text="Skip water (treat as transparent)", variable=self.skip_water_var).grid(row=2, column=2, columnspan=2, sticky="w", **pad)
        ttk.Label(opts, text="Hill shading:").grid(row=3, column=2, sticky="w", **pad)
        ttk.Combobox(opts, textvariable=self.hillshade_var, values=["none", "normal", "strong"], state="readonly", style="ActiveReadonly.TCombobox", width=12).grid(row=3, column=3, sticky="w", **pad)
        ttk.Checkbutton(opts, text="Use palette loaded in palette editor",
                        variable=self.use_editor_palette_var).grid(
            row=4, column=2, columnspan=2, sticky="w", **pad)

        # Crop
        crop = ttk.LabelFrame(parent, text="Crop / limit render area (block coordinates)")
        crop.pack(fill="x", padx=10, pady=6)

        ttk.Checkbutton(crop, text="Enable crop", variable=self.limit_enabled_var).grid(row=0, column=0, sticky="w", **pad)
        ttk.Label(crop, text="NW (x,z):").grid(row=0, column=1, sticky="w", **pad)
        ttk.Entry(crop, textvariable=self.xmin_var, width=10).grid(row=0, column=2, sticky="w", **pad)
        ttk.Entry(crop, textvariable=self.zmin_var, width=10).grid(row=0, column=3, sticky="w", **pad)
        ttk.Label(crop, text="SE (x,z):").grid(row=0, column=4, sticky="w", **pad)
        ttk.Entry(crop, textvariable=self.xmax_var, width=10).grid(row=0, column=5, sticky="w", **pad)
        ttk.Entry(crop, textvariable=self.zmax_var, width=10).grid(row=0, column=6, sticky="w", **pad)

        # Output naming / frames retention
        outopts = ttk.LabelFrame(parent, text="Output")
        outopts.pack(fill="x", padx=10, pady=6)

        ttk.Label(outopts, text="GIF name (optional):").grid(row=0, column=0, sticky="w", **pad)
        ttk.Entry(outopts, textvariable=self.output_name_var, width=40).grid(row=0, column=1, sticky="w", **pad)
        ttk.Label(outopts, text='(blank = auto: "WorldName_wmtt4mc.gif")').grid(row=0, column=2, sticky="w", **pad)

        ttk.Checkbutton(outopts, text="Keep frame PNGs after GIF is created", variable=self.keep_frames_var).grid(row=1, column=0, columnspan=3, sticky="w", **pad)
        ttk.Label(outopts, text="Output cache mode:").grid(row=2, column=0, sticky="w", **pad)
        _cache_row = ttk.Frame(outopts)
        _cache_row.grid(row=2, column=1, sticky="w", **pad)
        self.cache_mode_combo = ttk.Combobox(
            _cache_row,
            textvariable=self.cache_mode_var,
            values=[CACHE_MODE_SURFACE, CACHE_MODE_ALL_BLOCKS, CACHE_MODE_NONE],
            state="readonly",
            style="CacheMode.TCombobox",
            width=18,
        )
        self.cache_mode_combo.pack(side="left")
        _help_bg = self.style.lookup("TLabelframe", "background") or self.cget("background")
        _help_lbl = tk.Label(
            _cache_row,
            text="?",
            font=("Segoe UI", 8),
            bd=1,
            relief="solid",
            padx=3,
            pady=0,
            cursor="hand2",
            bg=_help_bg,
        )
        _help_lbl.pack(side="left", padx=(5, 0))
        _help_lbl.bind("<Button-1>", lambda _e: self.on_cache_mode_help())

        ttk.Label(outopts, text="Cache dimensions:").grid(row=3, column=0, sticky="w", **pad)
        dim_check_frame = ttk.Frame(outopts)
        dim_check_frame.grid(row=3, column=1, columnspan=2, sticky="w", **pad)
        self.cache_dim_checks = []
        for _var, _lbl in [
            (self.cache_dim_overworld_var, "Overworld"),
            (self.cache_dim_nether_var, "Nether"),
            (self.cache_dim_end_var, "The End"),
        ]:
            _cb = ttk.Checkbutton(dim_check_frame, text=_lbl, variable=_var)
            _cb.pack(side="left", padx=(0, 14))
            self.cache_dim_checks.append(_cb)
        self.cache_mode_var.trace_add("write", self._update_cache_dim_state)

        ttk.Label(outopts, textvariable=self.cache_crop_note_var, foreground="#a04a00").grid(row=4, column=0, columnspan=3, sticky="w", **pad)

        # Log + progress
        # Bottom controls (progress, buttons, advanced)
        bottom = ttk.Frame(parent)
        bottom.pack(fill="both", expand=True, padx=10, pady=8)

        self.progress = ttk.Progressbar(bottom, variable=self.progress_var, maximum=100.0)
        self.progress.pack(fill="x", expand=True)

        ttk.Label(bottom, textvariable=self.status1_var).pack(anchor="w")
        ttk.Label(bottom, textvariable=self.status2_var).pack(anchor="w")

        buttons = ttk.Frame(bottom)
        buttons.pack(fill="x", pady=(6, 0))
        self.go_btn = ttk.Button(buttons, text="▶ Render timelapse", style="Go.TButton", command=self.on_run)
        self.go_btn.pack(side="left")
        self.cache_btn = ttk.Button(buttons, text="Build / update caches", command=self.on_build_caches)
        self.cache_btn.pack(side="left", padx=(8, 0))
        ttk.Button(buttons, text="Stop", command=self.on_cancel).pack(side="left", padx=8)

        # Advanced toggle moved to bottom
        adv_toggle = ttk.Button(buttons, text="Advanced ▾", command=self.toggle_advanced)
        adv_toggle.pack(side="right")

        self.adv_frame = ttk.LabelFrame(bottom, text="Advanced")
        self.adv_frame.pack(fill="x", pady=(8, 0))
        self.adv_frame.pack_forget()

        self.adv_frame.columnconfigure(0, weight=0)
        self.adv_frame.columnconfigure(1, weight=0)
        self.adv_frame.columnconfigure(2, weight=1)

        ttk.Label(self.adv_frame, text="Performance tuning:").grid(row=0, column=0, sticky="w", **pad)
        ttk.Label(self.adv_frame, text="Automatic (hardware + live learning)").grid(row=0, column=1, columnspan=2, sticky="w", **pad)
        ttk.Label(self.adv_frame, text="Manual worker/scan toggles are disabled by design.").grid(row=1, column=0, columnspan=3, sticky="w", **pad)

        ttk.Label(self.adv_frame, text="FPS (advanced):").grid(row=1, column=0, sticky="w", **pad)
        ttk.Entry(self.adv_frame, textvariable=self.fps_var, width=10).grid(row=1, column=1, sticky="w", **pad)

        ttk.Checkbutton(self.adv_frame, text="Debug block IDs + unknowns", variable=self.debug_blocks_var).grid(row=2, column=0, columnspan=2, sticky="w", **pad)
        self.debug_btn = ttk.Button(self.adv_frame, text="Debug: render 1 chunk…", command=self.on_debug_one_chunk)
        self.debug_btn.grid(row=2, column=2, sticky="w", **pad)
        self.preflight_btn = ttk.Button(self.adv_frame, text="Run preflight report…", command=self.on_run_preflight)
        self.preflight_btn.grid(row=3, column=2, sticky="w", **pad)
        # Log output (hidden in Advanced by default)
        self.adv_frame.columnconfigure(0, weight=1)
        self.adv_frame.rowconfigure(4, weight=1)
        logbox = ttk.LabelFrame(self.adv_frame, text="Log")
        logbox.grid(row=4, column=0, columnspan=3, sticky="nsew", padx=0, pady=(8, 0))
        sb = ttk.Scrollbar(logbox, orient="vertical")
        sb.pack(side="right", fill="y")
        self.log_text = tk.Text(logbox, height=10, wrap="word", yscrollcommand=sb.set)
        sb.config(command=self.log_text.yview)
        self.log_text.pack(fill="both", expand=True, padx=6, pady=6)

        # Hide custom W/H unless Custom selected
        self.target_var.trace_add("write", lambda *_: self._sync_custom())
        self.target_var.trace_add("write", lambda *_: self._sync_cache_controls_for_crop())
        self._sync_custom()
        self._sync_debug_button_visibility()
        self.debug_blocks_var.trace_add("write", lambda *_: self._sync_debug_button_visibility())
        self.limit_enabled_var.trace_add("write", lambda *_: self._sync_cache_controls_for_crop())
        self._sync_cache_controls_for_crop()

    def _build_single_tab(self, parent):
        pad = {"padx": 8, "pady": 6}

        top = ttk.Frame(parent)
        top.pack(fill="x", **pad)

        ttk.Label(top, text="Backup / cache file:").grid(row=0, column=0, sticky="w", **pad)
        ttk.Entry(top, textvariable=self.single_zip_var, width=70).grid(row=0, column=1, sticky="we", **pad)
        ttk.Button(top, text="Browse…", command=self.pick_single_zip).grid(row=0, column=2, sticky="w", **pad)

        ttk.Label(top, text="Output PNG:").grid(row=1, column=0, sticky="w", **pad)
        ttk.Entry(top, textvariable=self.single_out_png_var, width=70).grid(row=1, column=1, sticky="we", **pad)
        ttk.Button(top, text="Browse…", command=self.pick_single_out).grid(row=1, column=2, sticky="w", **pad)
        top.columnconfigure(1, weight=1)

        opts = ttk.LabelFrame(parent, text="Map settings")
        opts.pack(fill="x", padx=10, pady=8)

        ttk.Label(opts, text="Dimension:").grid(row=0, column=0, sticky="w", **pad)
        ttk.Combobox(opts, textvariable=self.single_dimension_var,
                     values=["minecraft:overworld", "minecraft:the_nether", "minecraft:the_end"],
                     state="readonly", width=22).grid(row=0, column=1, sticky="w", **pad)

        ttk.Label(opts, text="Resolution:").grid(row=0, column=2, sticky="w", **pad)
        ttk.Combobox(opts, textvariable=self.single_target_var,
                     values=["Original (no scaling)", "1080p (1920x1080)", "4K (3840x2160)", "Custom…"],
                     state="readonly", width=22).grid(row=0, column=3, sticky="w", **pad)

        ttk.Label(opts, text="Custom W×H:").grid(row=1, column=2, sticky="w", **pad)
        self.single_custom_wh = ttk.Frame(opts)
        self.single_custom_wh.grid(row=1, column=3, sticky="w", **pad)
        ttk.Entry(self.single_custom_wh, textvariable=self.single_custom_w_var, width=8).pack(side="left")
        ttk.Label(self.single_custom_wh, text="×").pack(side="left", padx=4)
        ttk.Entry(self.single_custom_wh, textvariable=self.single_custom_h_var, width=8).pack(side="left")

        ttk.Label(opts, text="Y min / max:").grid(row=1, column=0, sticky="w", **pad)
        yrow = ttk.Frame(opts)
        yrow.grid(row=1, column=1, sticky="w", **pad)
        ttk.Entry(yrow, textvariable=self.single_ymin_var, width=6).pack(side="left")
        ttk.Label(yrow, text="to").pack(side="left", padx=4)
        ttk.Entry(yrow, textvariable=self.single_ymax_var, width=6).pack(side="left")

        ttk.Checkbutton(opts, text="Skip water", variable=self.single_skip_water_var).grid(row=2, column=0, sticky="w", **pad)
        ttk.Label(opts, text="Hill shading:").grid(row=2, column=1, sticky="w", **pad)
        ttk.Combobox(opts, textvariable=self.single_hillshade_var, values=["none", "normal", "strong"], state="readonly", width=12).grid(row=2, column=2, sticky="w", **pad)
        ttk.Checkbutton(opts, text="Use palette loaded in palette editor",
                        variable=self.single_use_editor_palette_var).grid(
            row=3, column=0, columnspan=3, sticky="w", **pad)

        crop = ttk.LabelFrame(parent, text="Crop / limit render area (block coordinates)")
        crop.pack(fill="x", padx=10, pady=6)

        ttk.Checkbutton(crop, text="Enable crop", variable=self.single_limit_enabled_var).grid(row=0, column=0, sticky="w", **pad)
        ttk.Label(crop, text="NW (x,z):").grid(row=0, column=1, sticky="w", **pad)
        ttk.Entry(crop, textvariable=self.single_xmin_var, width=10).grid(row=0, column=2, sticky="w", **pad)
        ttk.Entry(crop, textvariable=self.single_zmin_var, width=10).grid(row=0, column=3, sticky="w", **pad)
        ttk.Label(crop, text="SE (x,z):").grid(row=0, column=4, sticky="w", **pad)
        ttk.Entry(crop, textvariable=self.single_xmax_var, width=10).grid(row=0, column=5, sticky="w", **pad)
        ttk.Entry(crop, textvariable=self.single_zmax_var, width=10).grid(row=0, column=6, sticky="w", **pad)

        bottom = ttk.Frame(parent)
        bottom.pack(fill="x", padx=10, pady=10)
        ttk.Button(bottom, text="Render PNG", style="Go.TButton", command=self.on_render_single).pack(side="left")
        ttk.Button(bottom, text="Cancel", command=self.on_cancel).pack(side="left", padx=8)

        # Single tab custom visibility
        self.single_target_var.trace_add("write", lambda *_: self._sync_single_custom())
        self._sync_single_custom()

        # Advanced (single render) - hides log by default
        self.single_adv_open = tk.BooleanVar(value=False)

        def _toggle_single_adv():
            if self.single_adv_open.get():
                self.single_adv_frame.pack(fill="both", expand=True, padx=10, pady=6)
                self.single_adv_btn.config(text="Advanced ▴")
            else:
                self.single_adv_frame.pack_forget()
                self.single_adv_btn.config(text="Advanced ▾")
            self._resize_to_fit()

        def _on_single_adv_click():
            self.single_adv_open.set(not self.single_adv_open.get())
            _toggle_single_adv()

        self.single_adv_btn = ttk.Button(bottom, text="Advanced ▾", command=_on_single_adv_click)
        self.single_adv_btn.pack(side="right")

        self.single_adv_frame = ttk.LabelFrame(parent, text="Advanced")

        logbox = ttk.LabelFrame(self.single_adv_frame, text="Log")
        logbox.pack(fill="both", expand=True, padx=0, pady=0)
        sb = ttk.Scrollbar(logbox, orient="vertical")
        sb.pack(side="right", fill="y")
        self.single_log_text = tk.Text(logbox, height=10, wrap="word", yscrollcommand=sb.set)
        sb.config(command=self.single_log_text.yview)
        self.single_log_text.pack(fill="both", expand=True)

        _toggle_single_adv()

    def _prompt_cache_conflicts(
        self, snapshots: List[SnapshotInput]
    ) -> Optional[List[SnapshotInput]]:
        """Show a dialog when a same-stem cache and raw backup disagree on content hash.
        Returns the (possibly modified) snapshot list, or None if the user cancelled.
        """
        conflicts = [
            s for s in snapshots
            if s.warning and s.cache_path and s.kind != "cache"
        ]
        if not conflicts:
            return snapshots

        names = "\n".join(f"  \u2022  {s.display_name}" for s in conflicts)
        result: List[str] = []
        dlg = tk.Toplevel(self)
        dlg.title("Cache / Backup Mismatch")
        dlg.resizable(False, False)
        dlg.grab_set()

        msg = (
            "The following backups each have a .wmtt4mc cache file whose\n"
            "content does not match the backup (the cache may be outdated\n"
            "or built from a different version of the backup):\n\n"
            f"{names}\n\n"
            "How do you want to handle these?"
        )
        tk.Label(dlg, text=msg, justify="left", anchor="w").pack(
            padx=20, pady=(16, 10), anchor="w"
        )

        btn_row = ttk.Frame(dlg)
        btn_row.pack(pady=(0, 16), padx=20, anchor="w")

        def _pick(choice: str) -> None:
            result.append(choice)
            dlg.destroy()

        ttk.Button(btn_row, text="Use Raw Backups", command=lambda: _pick("raw")).pack(
            side="left", padx=(0, 8)
        )
        ttk.Button(btn_row, text="Use Cache Files", command=lambda: _pick("cache")).pack(
            side="left", padx=(0, 8)
        )
        ttk.Button(btn_row, text="Cancel", command=lambda: _pick("cancel")).pack(side="left")

        dlg.wait_window()

        choice = result[0] if result else "cancel"
        if choice == "cancel":
            return None
        if choice == "cache":
            for s in conflicts:
                s.raw_path = s.path
                s.path = s.cache_path
                s.kind = "cache"
                s.warning = ""
        else:
            # "raw" — clear the warning and proceed with raw backups
            for s in conflicts:
                s.warning = ""
        return snapshots

    def _maybe_show_first_run_help(self) -> None:
        """On first launch, switch to the Help tab so the user discovers it."""
        try:
            cfg = _load_config()
            if not cfg.get("first_run_help_shown", False):
                self.nb.select(self.tab_help)
                cfg["first_run_help_shown"] = True
                _save_config(cfg)
        except Exception:
            pass

    # -------------------------------------------------------------------------
    # Palette Editor tab
    # -------------------------------------------------------------------------

    def _build_palette_tab(self, parent: ttk.Frame) -> None:
        """Build the block-palette editor UI inside *parent*."""
        self._pal = _PaletteEditorState()
        self._pal_new_rgb: Optional[_PalRGB] = None

        # ---- Toolbar ----
        bar = ttk.Frame(parent)
        bar.pack(fill="x", padx=10, pady=(8, 4))
        ttk.Button(bar, text="Open palette.json…", command=self._pal_open).pack(side="left")
        ttk.Button(bar, text="Load unknowns from run…", command=self._pal_load_unknowns).pack(side="left", padx=(8, 0))
        ttk.Button(bar, text="From texture pack…", command=self._pal_from_texture_pack).pack(side="left", padx=(8, 0))
        ttk.Button(bar, text="Reload app palette", command=self._pal_reload_default).pack(side="left", padx=(8, 0))
        ttk.Button(bar, text="Save", command=self._pal_save).pack(side="left", padx=(8, 0))
        ttk.Button(bar, text="Save As…", command=self._pal_save_as).pack(side="left", padx=(8, 0))
        self._pal_path_lbl = ttk.Label(bar, text="(no file loaded)", foreground="#666")
        self._pal_path_lbl.pack(side="left", padx=(14, 0))

        # ---- Main area: left list + right editor ----
        main = ttk.Frame(parent)
        main.pack(fill="both", expand=True, padx=10, pady=4)

        # -- Left: block list --
        left = ttk.Frame(main)
        left.pack(side="left", fill="y", padx=(0, 10))

        sort_row = ttk.Frame(left)
        sort_row.pack(fill="x")
        ttk.Label(sort_row, text="Sort:").pack(side="left")
        self._pal_sort_var = tk.StringVar(value="Grouped")
        ttk.Combobox(sort_row, textvariable=self._pal_sort_var,
                     values=["Grouped", "Alphabetical"],
                     state="readonly", width=13).pack(side="left", padx=(4, 0))
        self._pal_sort_var.trace_add("write", lambda *_: self._pal_refresh_list())

        self._pal_search_var = tk.StringVar()
        search_ent = ttk.Entry(left, textvariable=self._pal_search_var)
        search_ent.pack(fill="x", pady=(4, 6))
        search_ent.bind("<KeyRelease>", lambda _e: self._pal_refresh_list())

        list_wrap = ttk.Frame(left)
        list_wrap.pack(fill="both", expand=True)
        self._pal_listbox = tk.Listbox(list_wrap, width=40, height=26, exportselection=False)
        self._pal_listbox.pack(side="left", fill="both", expand=True)
        lsb = ttk.Scrollbar(list_wrap, orient="vertical",
                             command=self._pal_listbox.yview,
                             style="WMTT.Vertical.TScrollbar")
        lsb.pack(side="left", fill="y")
        self._pal_listbox.configure(yscrollcommand=lsb.set)
        self._pal_listbox.bind("<<ListboxSelect>>", lambda _e: self._pal_on_select())
        # Scoped mousewheel for the listbox
        self._pal_listbox.bind("<Enter>",
            lambda _e: self._pal_listbox.bind_all("<MouseWheel>", self._pal_lbox_mw))
        self._pal_listbox.bind("<Leave>",
            lambda _e: self._pal_listbox.unbind_all("<MouseWheel>"))

        # -- Right: editor pane --
        right = ttk.Frame(main)
        right.pack(side="left", fill="both", expand=True)

        hdr = ttk.Frame(right)
        hdr.pack(fill="x")
        self._pal_selected_lbl = ttk.Label(hdr, text="(select a block)",
                                           font=("Segoe UI", 11, "bold"))
        self._pal_selected_lbl.pack(side="left")
        self._pal_copy_btn = ttk.Button(hdr, text="Copy color to other blocks…",
                                        command=self._pal_copy_to_others, state="disabled")
        self._pal_copy_btn.pack(side="right")

        mid = ttk.Frame(right)
        mid.pack(fill="both", expand=True, pady=(10, 0))

        # Swatches + controls (left of picker)
        ctrl = ttk.Frame(mid)
        ctrl.pack(side="left", fill="y", padx=(0, 14))

        ttk.Label(ctrl, text="Current").grid(row=0, column=0, sticky="w")
        ttk.Label(ctrl, text="New").grid(row=0, column=1, sticky="w", padx=(14, 0))

        self._pal_cur_swatch = tk.Canvas(ctrl, width=110, height=110,
                                         highlightthickness=1, highlightbackground="#999")
        self._pal_new_swatch = tk.Canvas(ctrl, width=110, height=110,
                                         highlightthickness=1, highlightbackground="#999")
        self._pal_cur_swatch.grid(row=1, column=0, pady=(4, 0))
        self._pal_new_swatch.grid(row=1, column=1, padx=(14, 0), pady=(4, 0))

        self._pal_cur_lbl = ttk.Label(ctrl, text="—")
        self._pal_new_lbl = ttk.Label(ctrl, text="—")
        self._pal_cur_lbl.grid(row=2, column=0, sticky="w", pady=(4, 0))
        self._pal_new_lbl.grid(row=2, column=1, sticky="w", padx=(14, 0), pady=(4, 0))

        # RGB inputs
        rgbf = ttk.LabelFrame(ctrl, text="RGB / Hex")
        rgbf.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(10, 0))
        self._pal_r_var = tk.StringVar()
        self._pal_g_var = tk.StringVar()
        self._pal_b_var = tk.StringVar()
        for row_i, (lbl, var) in enumerate([("R", self._pal_r_var),
                                             ("G", self._pal_g_var),
                                             ("B", self._pal_b_var)]):
            ttk.Label(rgbf, text=lbl).grid(row=row_i, column=0, sticky="w", padx=6, pady=3)
            ent = ttk.Entry(rgbf, textvariable=var, width=7)
            ent.grid(row=row_i, column=1, sticky="w", padx=6, pady=3)
            ent.bind("<KeyRelease>", lambda _e: self._pal_rgb_changed())
        hex_row = ttk.Frame(rgbf)
        hex_row.grid(row=3, column=0, columnspan=2, sticky="ew", padx=6, pady=(4, 6))
        ttk.Label(hex_row, text="#").pack(side="left")
        self._pal_hex_var = tk.StringVar()
        hex_ent = ttk.Entry(hex_row, textvariable=self._pal_hex_var, width=8)
        hex_ent.pack(side="left", padx=(0, 6))
        hex_ent.bind("<Return>", lambda _e: self._pal_apply_hex())
        ttk.Button(hex_row, text="Read hex code", command=self._pal_apply_hex).pack(side="left")

        # Transparent + apply buttons
        self._pal_transparent_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(ctrl, text="Transparent (skip when rendering)",
                        variable=self._pal_transparent_var,
                        command=self._pal_transparent_toggled).grid(
            row=4, column=0, columnspan=2, sticky="w", pady=(10, 0))

        self._pal_apply_btn = ttk.Button(ctrl, text="Apply new color",
                                          command=self._pal_apply_new_color, state="disabled")
        self._pal_apply_btn.grid(row=5, column=0, columnspan=2, sticky="ew", pady=(8, 0))

        # HSV picker (right of controls)
        picker_frame = ttk.LabelFrame(mid, text="Hue / Saturation / Value")
        picker_frame.pack(side="left", anchor="n", pady=(0, 0))
        self._pal_picker = _HSVPicker(picker_frame, on_change=self._pal_picker_changed)
        self._pal_picker.pack(padx=10, pady=10, anchor="nw")

        # ---- Status bar ----
        self._pal_status_var = tk.StringVar(value="Open a palette.json to begin.")
        ttk.Label(parent, textvariable=self._pal_status_var,
                  anchor="w", foreground="#444").pack(fill="x", padx=10, pady=(4, 6))

        # Auto-load app palette on first show
        self.nb.bind("<<NotebookTabChanged>>", self._pal_on_tab_shown)

    def _pal_on_tab_shown(self, _evt=None) -> None:
        """Auto-load the app's palette.json the first time this tab is revealed."""
        try:
            if self.nb.select() != str(self.tab_palette):
                return
        except Exception:
            return
        # Only auto-load once (if no file is loaded yet)
        if self._pal.path is None:
            default = os.path.join(_app_dir(), "palette.json")
            if os.path.isfile(default):
                self._pal_load(default)
            else:
                self._pal_set_status("No palette.json found beside the app. Use 'Open palette.json…' to load one.")

    def _pal_lbox_mw(self, evt) -> None:
        step = -1 if getattr(evt, "delta", 0) > 0 else 1
        self._pal_listbox.yview_scroll(step, "units")

    def _pal_set_status(self, msg: str, *, unsaved: bool = False) -> None:
        prefix = "● " if unsaved else ""
        self._pal_status_var.set(prefix + msg)

    def _pal_refresh_list(self) -> None:
        if not self._pal.keys_sorted:
            self._pal_listbox.delete(0, tk.END)
            return
        q = self._pal_search_var.get().strip().lower()
        keys = list(self._pal.keys_sorted)
        if self._pal_sort_var.get() == "Grouped":
            keys.sort(key=lambda k: (_pal_derive_group(k), k))
        else:
            keys.sort()
        display_keys: List[str] = []
        display_to_key: Dict[str, str] = {}
        for k in keys:
            grp = _pal_derive_group(k) if self._pal_sort_var.get() == "Grouped" else ""
            label = f"{grp} / {k}" if grp else k
            if k in self._pal.transparent:
                label += "  [transparent]"
            display_keys.append(label)
            display_to_key[label] = k
        if q:
            display_keys = [d for d in display_keys if q in d.lower()]
        self._pal.display_keys = display_keys
        self._pal.display_to_key = display_to_key
        self._pal_listbox.delete(0, tk.END)
        for d in display_keys:
            self._pal_listbox.insert(tk.END, d)

    def _pal_selected_key(self) -> Optional[str]:
        sel = self._pal_listbox.curselection()
        if not sel:
            return None
        return self._pal.display_to_key.get(self._pal_listbox.get(sel[0]))

    def _pal_on_select(self) -> None:
        k = self._pal_selected_key()
        if not k:
            return
        rgb = self._pal.palette.get(k)
        if rgb is None:
            return
        self._pal_selected_lbl.configure(text=k)
        self._pal_set_current(rgb)
        self._pal_set_new(rgb)
        self._pal_picker.set_rgb(rgb)
        self._pal_transparent_var.set(k in self._pal.transparent)
        self._pal_apply_btn.configure(state="normal")
        self._pal_copy_btn.configure(state="normal")

    def _pal_set_current(self, rgb: _PalRGB) -> None:
        hex_ = _pal_rgb_to_hex(rgb)
        self._pal_cur_swatch.configure(bg=hex_)
        self._pal_cur_lbl.configure(text=f"{hex_}  ({rgb[0]},{rgb[1]},{rgb[2]})")

    def _pal_set_new(self, rgb: _PalRGB) -> None:
        self._pal_new_rgb = rgb
        hex_ = _pal_rgb_to_hex(rgb)
        self._pal_new_swatch.configure(bg=hex_)
        self._pal_new_lbl.configure(text=f"{hex_}  ({rgb[0]},{rgb[1]},{rgb[2]})")
        self._pal_r_var.set(str(rgb[0]))
        self._pal_g_var.set(str(rgb[1]))
        self._pal_b_var.set(str(rgb[2]))
        self._pal_hex_var.set(hex_[1:])  # without '#'

    def _pal_picker_changed(self, rgb: _PalRGB) -> None:
        self._pal_set_new(rgb)

    def _pal_rgb_changed(self) -> None:
        try:
            rgb = (_pal_clamp(int(self._pal_r_var.get())),
                   _pal_clamp(int(self._pal_g_var.get())),
                   _pal_clamp(int(self._pal_b_var.get())))
        except Exception:
            return
        self._pal_new_rgb = rgb
        hex_ = _pal_rgb_to_hex(rgb)
        self._pal_new_swatch.configure(bg=hex_)
        self._pal_new_lbl.configure(text=f"{hex_}  ({rgb[0]},{rgb[1]},{rgb[2]})")
        self._pal_hex_var.set(hex_[1:])
        self._pal_picker.set_rgb(rgb)

    def _pal_apply_hex(self) -> None:
        raw = self._pal_hex_var.get().strip().lstrip("#")
        rgb = _pal_hex_to_rgb(raw)
        if rgb is None:
            messagebox.showerror("Invalid hex", "Enter a 3 or 6-digit hex color (e.g. ff8800).",
                                 parent=self)
            return
        self._pal_set_new(rgb)
        self._pal_picker.set_rgb(rgb)

    def _pal_transparent_toggled(self) -> None:
        k = self._pal_selected_key()
        if not k:
            return
        if self._pal_transparent_var.get():
            self._pal.transparent.add(k)
        else:
            self._pal.transparent.discard(k)
        self._pal.unsaved_changes = True
        state = "transparent" if k in self._pal.transparent else "opaque"
        self._pal_set_status(f"Marked {k} as {state} — not saved.", unsaved=True)
        # Refresh list so [transparent] tag updates, then restore selection + scroll position.
        self._pal_refresh_list()
        for idx, label in enumerate(self._pal.display_keys):
            if self._pal.display_to_key.get(label) == k:
                self._pal_listbox.selection_set(idx)
                self._pal_listbox.see(idx)
                break

    def _pal_apply_new_color(self) -> None:
        k = self._pal_selected_key()
        if not k or self._pal_new_rgb is None:
            return
        self._pal.palette[k] = self._pal_new_rgb
        self._pal.unsaved_changes = True
        self._pal_set_current(self._pal_new_rgb)
        self._pal_set_status(f"Updated {k} → {_pal_rgb_to_hex(self._pal_new_rgb)} — not saved.",
                              unsaved=True)

    def _pal_copy_to_others(self) -> None:
        k = self._pal_selected_key()
        if not k or self._pal_new_rgb is None:
            return
        dlg = _PaletteCopyDialog(self, all_keys=self._pal.keys_sorted,
                                  current_key=k,
                                  current_transparent=k in self._pal.transparent)
        self.wait_window(dlg)
        targets = [t for t in dlg.result_keys if t != k]
        if not targets:
            return
        for t in targets:
            self._pal.palette[t] = self._pal_new_rgb
        if dlg.result_copy_transparent:
            if self._pal_transparent_var.get():
                self._pal.transparent.update(targets)
            else:
                for t in targets:
                    self._pal.transparent.discard(t)
        self._pal.unsaved_changes = True
        self._pal_set_status(
            f"Copied {_pal_rgb_to_hex(self._pal_new_rgb)} to {len(targets)} blocks — not saved.",
            unsaved=True)

    def _pal_open(self) -> None:
        path = filedialog.askopenfilename(
            title="Open palette.json",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
            parent=self,
        )
        if not path:
            return
        # If a palette is already loaded, ask whether to replace or merge.
        if self._pal.palette:
            dlg = tk.Toplevel(self)
            dlg.title("Open palette")
            dlg.transient(self)
            dlg.grab_set()
            dlg.resizable(False, False)
            dlg.geometry(f"+{self.winfo_rootx() + 80}+{self.winfo_rooty() + 80}")
            choice_var = tk.StringVar(value="")
            ttk.Label(
                dlg,
                text=(
                    f"A palette with {len(self._pal.palette):,} entries is already loaded.\n"
                    "Add entries from the new file (merge / resolve conflicts)\n"
                    "or replace the current palette entirely?"
                ),
                justify="left",
            ).pack(padx=16, pady=(14, 8))
            btn_row = ttk.Frame(dlg)
            btn_row.pack(padx=16, pady=(4, 14))
            ttk.Button(
                btn_row, text="Add to current",
                command=lambda: (choice_var.set("add"), dlg.destroy()),
            ).pack(side="left", padx=4)
            ttk.Button(
                btn_row, text="Replace",
                command=lambda: (choice_var.set("replace"), dlg.destroy()),
            ).pack(side="left", padx=4)
            ttk.Button(
                btn_row, text="Cancel",
                command=lambda: (choice_var.set("cancel"), dlg.destroy()),
            ).pack(side="left", padx=4)
            self.wait_window(dlg)
            choice = choice_var.get()
            if not choice or choice == "cancel":
                return
            if choice == "add":
                self._pal_load_merge(path)
                return
        self._pal_load(path)

    def _pal_merge_dict(
        self,
        incoming: Dict[str, _PalRGB],
        *,
        incoming_transparent: Optional[set] = None,
        source_label: str = "imported",
    ) -> None:
        """Merge *incoming* colors into the current palette.

        Exact duplicates (same key and same color) are silently skipped.
        Same key but different color → ``_PaletteConflictDialog`` lets the user choose.
        New keys are added unconditionally.  Aborts entirely if the conflict
        dialog is cancelled.
        """
        current = self._pal.palette
        added: Dict[str, _PalRGB] = {}
        conflicts: List[Tuple[str, _PalRGB, _PalRGB]] = []
        for k, new_rgb in incoming.items():
            if k not in current:
                added[k] = new_rgb
            elif current[k] != new_rgb:
                conflicts.append((k, current[k], new_rgb))
            # else: exact duplicate — silently skip

        resolutions: Dict[str, _PalRGB] = {}
        if conflicts:
            cdlg = _PaletteConflictDialog(self, conflicts)
            self.wait_window(cdlg)
            if cdlg.resolutions is None:
                return  # user cancelled
            resolutions = cdlg.resolutions

        self._pal.palette.update(added)
        self._pal.palette.update(resolutions)
        if incoming_transparent:
            self._pal.transparent.update(incoming_transparent)
        self._pal.keys_sorted = sorted(self._pal.palette.keys())
        self._pal.unsaved_changes = bool(added or resolutions)
        self._pal_refresh_list()
        conf_msg = f", {len(conflicts)} conflict(s) resolved" if conflicts else ""
        self._pal_set_status(
            f"Merged from {source_label}: {len(added)} block(s) added{conf_msg} — not saved.",
            unsaved=bool(added or resolutions),
        )

    def _pal_load_merge(self, path: str) -> None:
        """Load *path* as a palette file and merge into the current palette."""
        try:
            _raw_obj, incoming, incoming_transparent = _pal_load_file(path)
            if not incoming:
                raise ValueError("No valid RGB entries found.")
        except Exception as exc:
            messagebox.showerror(
                "Failed to open palette", f"{type(exc).__name__}: {exc}", parent=self
            )
            return
        self._pal_merge_dict(
            incoming,
            incoming_transparent=incoming_transparent,
            source_label=os.path.basename(path),
        )

    def _pal_from_texture_pack(self) -> None:
        """Extract block colors from a Java/Bedrock texture pack and import selected ones."""
        path = filedialog.askopenfilename(
            title="Open texture pack",
            filetypes=[
                ("Texture packs", "*.zip *.mcpack"),
                ("Zip archive", "*.zip"),
                ("Bedrock pack (.mcpack)", "*.mcpack"),
                ("All files", "*.*"),
            ],
            parent=self,
        )
        if not path:
            # Offer folder selection as alternative
            if not messagebox.askyesno(
                "No file selected",
                "Would you like to select an extracted texture pack folder instead?",
                parent=self,
            ):
                return
            path = filedialog.askdirectory(
                title="Open texture pack folder", parent=self
            ) or ""
        if not path:
            return

        # Build a progress dialog — extraction can take several seconds.
        prog = tk.Toplevel(self)
        prog.title("Scanning texture pack…")
        prog.resizable(False, False)
        prog.transient(self)
        prog.grab_set()
        # Centre relative to main window
        self.update_idletasks()
        px = self.winfo_rootx() + (self.winfo_width() - 320) // 2
        py = self.winfo_rooty() + (self.winfo_height() - 120) // 2
        prog.geometry(f"320x120+{px}+{py}")
        ttk.Label(prog, text=f"Reading: {os.path.basename(path)}",
                  wraplength=290).pack(padx=16, pady=(14, 4))
        pb = ttk.Progressbar(prog, mode="indeterminate", length=288)
        pb.pack(padx=16, pady=4)
        pb.start(12)
        cancel_flag = [False]
        ttk.Button(prog, text="Cancel",
                   command=lambda: cancel_flag.__setitem__(0, True)).pack(pady=(4, 10))

        # Run extraction on a daemon thread so the UI stays responsive.
        result: List = [None, None, None]  # [extracted, warnings, exception]

        def _worker() -> None:
            try:
                result[0], result[1] = _pal_extract_texture_pack(path)
            except Exception as exc:
                result[2] = exc

        t = threading.Thread(target=_worker, daemon=True)
        t.start()

        def _poll() -> None:
            if t.is_alive() and not cancel_flag[0]:
                self.after(150, _poll)
                return
            pb.stop()
            prog.destroy()
            if cancel_flag[0]:
                self._pal_set_status("Texture pack scan cancelled.")
                return
            exc: Optional[Exception] = result[2]
            if exc is not None:
                messagebox.showerror(
                    "Texture pack error",
                    f"Failed to read texture pack:\n{type(exc).__name__}: {exc}",
                    parent=self,
                )
                return
            extracted, warnings = result[0], result[1]
            if not extracted:
                messagebox.showinfo(
                    "No textures found",
                    "No block textures were found in this pack.\n\n"
                    "Java packs must have assets/*/textures/block[s]/ structure.\n"
                    "Bedrock packs must have textures/blocks/ structure.",
                    parent=self,
                )
                return
            pdlg = _TexturePackPreviewDialog(self, extracted, warnings)
            self.wait_window(pdlg)
            if not pdlg.result_palette:
                return
            self._pal_merge_dict(
                pdlg.result_palette,
                source_label=os.path.basename(path),
            )

        self.after(150, _poll)

    def _pal_load_unknowns(self) -> None:
        """Load an unknown_blocks.json written after a render run and add any missing blocks
        to the current palette with the gray placeholder color, then filter the list to show
        only those blocks so the user can quickly assign colors to them.
        """
        path = filedialog.askopenfilename(
            title="Open unknown_blocks.json from a render run",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
            initialfile="unknown_blocks.json",
            parent=self,
        )
        if not path:
            return

        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as exc:
            messagebox.showerror("Failed to read file",
                                 f"{type(exc).__name__}: {exc}", parent=self)
            return

        if not isinstance(data, dict) or not data:
            messagebox.showerror("Invalid file",
                                 "Expected a JSON object mapping block IDs to counts.",
                                 parent=self)
            return

        # Ensure a palette is loaded; create a minimal one if not
        if not self._pal.palette:
            self._pal.palette = {}
            self._pal.raw_obj = {"schema_version": 1, "rgb_overrides": {}, "transparent_blocks": []}
            self._pal.transparent = set()
            self._pal.keys_sorted = []
            self._pal.unsaved_changes = False

        # Add unknown blocks that are not already present with the gray placeholder (180,180,180)
        added: list = []
        for block_id in data:
            bid = str(block_id).strip()
            if not bid:
                continue
            if bid not in self._pal.palette:
                self._pal.palette[bid] = (180, 180, 180)
                added.append(bid)

        if added:
            self._pal.keys_sorted = sorted(self._pal.palette.keys())
            self._pal.unsaved_changes = True

        # Preload the search filter with the unknown block IDs so they're immediately visible
        unknown_ids = sorted(str(b) for b in data if str(b).strip())
        # Use a common prefix substring if possible, otherwise leave blank so the user sees
        # all entries; we'll filter the listbox to show only the loaded unknowns.
        self._pal_search_var.set("")
        self._pal_refresh_list()

        # Re-filter to show only the unknowns just loaded (display_keys holds label strings;
        # use display_to_key to map back to the raw block ID for the membership check)
        unknown_set = set(unknown_ids)
        self._pal.display_keys = [
            d for d in self._pal.display_keys
            if self._pal.display_to_key.get(d) in unknown_set
        ]
        self._pal_listbox.delete(0, "end")
        for d in self._pal.display_keys:
            self._pal_listbox.insert("end", d)

        n_unknown = len(unknown_ids)
        n_added = len(added)
        msg = (
            f"Loaded {n_unknown} unknown block(s) from run ({n_added} new)."
            if n_added else
            f"{n_unknown} unknown block(s) loaded — all already in palette."
        )
        if n_added:
            msg += " Assign colors and Save."
        self._pal_set_status(msg, unsaved=bool(added))

    def _pal_reload_default(self) -> None:
        """Load (or reload) the app's built-in palette.json from the app directory."""
        path = os.path.join(_app_dir(), "palette.json")
        if not os.path.isfile(path):
            messagebox.showinfo("Not found",
                                f"No palette.json found at:\n{path}\n\n"
                                "Run the app once to generate a default palette, "
                                "then click 'Reload app palette'.",
                                parent=self)
            return
        self._pal_load(path)

    def _pal_load(self, path: str) -> None:
        try:
            raw_obj, palette, transparent = _pal_load_file(path)
            if not palette:
                raise ValueError("No valid RGB entries found.")
            self._pal.path = path
            self._pal.palette = palette
            self._pal.raw_obj = raw_obj
            self._pal.transparent = transparent
            self._pal.keys_sorted = sorted(palette.keys())
            self._pal.unsaved_changes = False
            self._pal_path_lbl.configure(text=os.path.abspath(path))
            self._pal_refresh_list()
            self._pal_selected_lbl.configure(text="(select a block)")
            self._pal_apply_btn.configure(state="disabled")
            self._pal_copy_btn.configure(state="disabled")
            self._pal_set_status(
                f"Loaded {len(palette):,} entries"
                f"{f', {len(transparent)} transparent' if transparent else ''}"
                f" — {os.path.basename(path)}"
            )
        except Exception as exc:
            messagebox.showerror("Failed to open palette",
                                 f"{type(exc).__name__}: {exc}", parent=self)

    def _pal_save(self) -> None:
        if not self._pal.path:
            self._pal_save_as()
            return
        self._pal_write(self._pal.path)

    def _pal_save_as(self) -> None:
        path = filedialog.asksaveasfilename(
            title="Save palette as…",
            defaultextension=".json",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
            parent=self,
        )
        if path:
            self._pal_write(path)
            self._pal.path = path
            self._pal_path_lbl.configure(text=os.path.abspath(path))

    def _pal_write(self, path: str) -> None:
        try:
            _pal_write_file(path, self._pal.raw_obj, self._pal.palette, self._pal.transparent)
            self._pal.unsaved_changes = False
            self._pal_set_status(
                f"Saved {len(self._pal.palette):,} entries to {os.path.basename(path)}"
            )
            # If we just saved the app's palette.json, reload it into memory
            app_palette = os.path.normcase(os.path.abspath(
                os.path.join(_app_dir(), "palette.json")))
            if os.path.normcase(os.path.abspath(path)) == app_palette:
                try:
                    apply_palette_overrides(path)
                    self._pal_set_status(
                        f"Saved and reloaded {len(self._pal.palette):,} entries "
                        f"— rendering will use the updated palette."
                    )
                except Exception:
                    pass
        except Exception as exc:
            messagebox.showerror("Save failed", f"{type(exc).__name__}: {exc}", parent=self)

    def _build_help_tab(self, parent: ttk.Frame) -> None:
        """Populate the Help / Info tab with formatted usage documentation."""
        import tkinter.font as tkfont

        sb = ttk.Scrollbar(parent, orient="vertical", style="WMTT.Vertical.TScrollbar")
        sb.pack(side="right", fill="y")
        txt = tk.Text(
            parent, wrap="word", yscrollcommand=sb.set,
            padx=16, pady=12, cursor="arrow", relief="flat",
        )
        txt.pack(fill="both", expand=True)
        sb.config(command=txt.yview)

        try:
            base = tkfont.nametofont("TkDefaultFont").actual()
            fam = base.get("family", "Segoe UI")
            sz = max(9, int(base.get("size", 10)))
        except Exception:
            fam, sz = "Segoe UI", 10

        txt.tag_configure("h1",   font=(fam, sz + 4, "bold"),  spacing1=14, spacing3=4)
        txt.tag_configure("h2",   font=(fam, sz + 1, "bold"),  spacing1=10, spacing3=2, foreground="#1a5f9e")
        txt.tag_configure("body", font=(fam, sz),               spacing1=0,  spacing3=0)
        txt.tag_configure("li",   font=(fam, sz),               lmargin1=20, lmargin2=36, spacing1=1)
        txt.tag_configure("code", font=("Courier New", sz),     foreground="#444444")
        txt.tag_configure("rule", font=(fam, sz - 2),           foreground="#bbbbbb")
        txt.tag_configure("note", font=(fam, sz - 1, "italic"), foreground="#777777")

        i = txt.insert
        RL = "\u2500" * 65

        def sec(title: str) -> None:
            i("end", RL + "\n", "rule")
            i("end", title + "\n", "h2")
            i("end", RL + "\n", "rule")

        def li(text: str) -> None:
            i("end", "  \u2022  " + text + "\n", "li")

        i("end", f"WMTT4MC  v{APP_VERSION}  \u2014  World Map Timeline Tool for Minecraft\n", "h1")
        i("end", "\n")
        i("end",
          "WMTT4MC reads Minecraft world backups and renders top-down map images,\n"
          "then assembles them into an animated GIF timelapse showing how your world\n"
          "has changed over time.\n\n", "body")

        sec("Quick Start  (Timelapse)")
        i("end", "1.  Backups / caches folder  \u2192  Browse\u2026 to a folder of world backup ZIPs.\n", "body")
        i("end", "2.  Output folder  \u2192  Browse\u2026 to where you want frames and the GIF saved.\n", "body")
        i("end", "3.  Adjust settings if needed, then click  \u25b6 Render timelapse.\n\n", "body")
        i("end",
          "The app renders one PNG per backup (newest first) then stitches them\n"
          "into an animated GIF.\n\n", "body")

        sec("Supported Input Formats")
        li("ZIP files containing a world folder  (e.g. World_Backup-2025-01-01.zip)")
        li("Extracted world folders with a level.dat file")
        li(".wmtt4mc cache files  \u2014  see Cache Files section below")
        i("end", "\n")

        sec("Cache Files  (.wmtt4mc)")
        i("end",
          "Cache files store pre-scanned block data from a world backup.\n"
          "They are typically 5\u201320 MB rather than hundreds of MB for a raw ZIP\n"
          "\u2014 much faster to render from on repeated runs.\n\n", "body")
        i("end", "How caches work:\n", "body")
        li("Click \u201cBuild / update caches\u201d to generate .wmtt4mc files next to each backup.")
        li("On the next render, WMTT4MC uses the cache instead of re-scanning the ZIP.")
        li("Once cached, you can delete the original ZIP to reclaim disk space.")
        li("If the backup content changes, the cache is flagged as mismatched and\n"
           "      you are prompted to decide what to do.")
        i("end", "\n")
        i("end", "Output cache mode  (Output section \u2192 Output cache mode drop-down):\n", "body")
        i("end", "  surface     ", "code")
        i("end", " \u2014  Default. Top visible block per column. Fastest build, smallest file.\n", "body")
        i("end", "  all_blocks  ", "code")
        i("end", " \u2014  Also stores full column segments (~3\u20135\u00d7 larger). Allows\n"
                 "               re-rendering at different Y ranges without rebuilding.\n", "body")
        i("end", "  none        ", "code")
        i("end", " \u2014  Don't build new cache files. Existing .wmtt4mc files are still\n"
                 "               used automatically if present in the source folder.\n\n", "body")

        sec("Settings Explained")
        i("end", "Dimension\n", "h2")
        i("end", "  Which Minecraft dimension to render.\n", "body")
        li("minecraft:overworld   \u2014  The main world (default)")
        li("minecraft:the_nether  \u2014  The Nether  (Y 0\u2013128 recommended)")
        li("minecraft:the_end     \u2014  The End")
        i("end", "\n")
        i("end", "Y min / max\n", "h2")
        i("end",
          "  The block height range scanned for the top visible block.\n"
          "  Default 0\u2013320 covers the full Overworld. Raise Y min to skip underground;\n"
          "  lower Y max to ignore very high builds or open the Nether ceiling.\n\n", "body")
        i("end", "Video resolution\n", "h2")
        li("Original          \u2014  1 px per block column, no scaling")
        li("720p / 1080p / 4K \u2014  scaled to fit the selected frame size")
        li("Custom\u2026           \u2014  enter your own pixel dimensions")
        i("end", "\n")
        i("end", "Seconds per frame\n", "h2")
        i("end",
          "  How long each backup frame is shown in the GIF (lower = faster animation).\n"
          "  Default: 1.0 s.  The Advanced \u203a FPS field updates this automatically.\n\n", "body")
        i("end", "Skip water\n", "h2")
        i("end", "  Treats water as transparent, revealing the sea floor and underwater builds.\n\n", "body")
        i("end", "Hillshade\n", "h2")
        i("end", "  Adds height-based shading for a 3D terrain look.  On by default.\n\n", "body")
        i("end", "Crop / limit render area\n", "h2")
        i("end",
          "  Restrict the map to a block-coordinate bounding box. Useful for focusing on\n"
          "  a region and reducing render time.\n\n", "body")
        i("end", "GIF name\n", "h2")
        i("end", "  Optional custom output filename.  Default: auto-derived from the world name.\n\n", "body")
        i("end", "Keep frame PNGs\n", "h2")
        i("end",
          "  Frame PNGs are deleted after the GIF is created by default.\n"
          "  Enable this to keep them.\n\n", "body")

        sec("Tips")
        li("Name backups with dates or sequential numbers so the timelapse is in order.\n"
           "      e.g.  World_Backup-2025-01-01.zip,  World_Backup-2025-02-01.zip")
        li("Run \u201cBuild / update caches\u201d before a large render batch \u2014 future renders\n"
           "      will be much faster and you can then delete the original ZIPs.")
        li("Both Java Edition and Bedrock Edition world formats are supported.")
        li("You can stop any render at any time with the Stop button.")
        li("Detailed log output (including errors) appears in  Advanced \u203a Log.")
        i("end", "\n")

        sec("Palette Editor")
        i("end",
          "The Palette Editor tab lets you control which colour is used for each block\n"
          "when rendering.  WMTT4MC ships with a built-in palette of over 1 100 block\n"
          "colours derived from Java Edition texture averages.\n\n", "body")

        i("end", "How palette lookup works during a render\n", "h2")
        i("end",
          "  For each block WMTT4MC encounters, colours are resolved in this order:\n\n", "body")
        i("end", "  1. ", "body")
        i("end", "Editor palette", "code")
        i("end",
          "  \u2014  if \u201cUse palette loaded in palette editor\u201d is ticked.\n"
          "        Only checked when a palette is actually loaded in the editor tab.\n", "body")
        i("end", "  2. ", "body")
        i("end", "Built-in palette", "code")
        i("end",
          "  \u2014  the default colours shipped with the app (or overrides from the\n"
          "        app\u2019s palette.json on disk if you have saved one there).\n", "body")
        i("end", "  3. ", "body")
        i("end", "Heuristics", "code")
        i("end",
          "  \u2014  pattern-matched fallbacks for leaves, logs, stone-like blocks, water, etc.\n", "body")
        i("end", "  4. ", "body")
        i("end", "Unknown grey  (180, 180, 180)", "code")
        i("end",
          "  \u2014  the block is logged to  unknown_blocks.json  in the run folder.\n\n", "body")

        i("end", "Using a custom palette for a render\n", "h2")
        li("Open the Palette Editor tab and load or build the palette you want.")
        li("Tick  \u201cUse palette loaded in palette editor\u201d  in the render options\n"
           "      (below the Hill shading control on both the Timelapse and Single Map tabs).")
        li("Click Render.  The editor palette is checked first; any block not found\n"
           "      there falls through to the built-in palette and heuristics.")
        li("You do \u2014not\u2014 need to save the palette to disk first.  Whatever is\n"
           "      currently in memory in the editor is used.")
        i("end",
          "\n  Note: the palette is snapshotted as a copy at the moment you click Render.\n"
          "  Changes made in the editor while a render is running do not affect that job.\n\n",
          "note")

        i("end", "Saving a palette so it is used by default (without ticking the checkbox)\n", "h2")
        li("Click  Save  or  Save As\u2026  in the Palette Editor toolbar.")
        li("Point the save dialog at  palette.json  next to the app  (or the .exe).")
        li("The app reloads those overrides into memory immediately after saving,\n"
           "      so subsequent renders pick them up automatically.")
        i("end", "\n")

        i("end", "Importing colours from a texture pack\n", "h2")
        i("end",
          "  Click  \u201cFrom texture pack\u2026\u201d  in the Palette Editor toolbar and select a\n"
          "  Java or Bedrock resource/texture pack \u2014 either a  .zip / .mcpack  file\n"
          "  or an extracted folder.  The app will:\n\n", "body")
        li("Scan block textures and average each texture\u2019s pixels by alpha weight.")
        li("Show a preview dialog where you can filter and tick which blocks to import.")
        li("Merge the selected colours into the editor, showing a conflict resolution\n"
           "      dialog for any block that already has a different colour loaded.")
        i("end",
          "\n  Animated textures (tall strips where height = n \u00d7 width) use only\n"
          "  the first frame.  Fully transparent textures are skipped.\n\n", "note")
        i("end",
          "  Java packs must have the structure:   assets/*/textures/block[s]/*.png\n"
          "  Bedrock packs must have the structure: textures/blocks/**/*.png  or  .tga\n\n",
          "code")

        i("end", "Loading and merging palette files\n", "h2")
        li("Open palette.json\u2026  \u2014  loads a palette file into the editor.")
        li("If a palette is already loaded you are asked: \u201cAdd to current\u201d or \u201cReplace\u201d.")
        li("Add to current:  identical entries are silently skipped; same block with\n"
           "      a different colour opens the conflict resolution dialog.")
        li("Load unknowns from run\u2026  \u2014  reads an  unknown_blocks.json  from a run\n"
           "      folder and adds every unknown block as grey so you can assign colours.")
        i("end", "\n")

        i("end", "Transparent blocks\n", "h2")
        i("end",
          "  Tick the \u201cTransparent\u201d checkbox next to any entry to mark that block\n"
          "  as transparent \u2014 it will be skipped during rendering (like glass or air).\n"
          "  Transparent entries are stored in the  transparent  list inside the\n"
          "  palette.json file.\n\n", "body")

        sec("Tips")
        li("Name backups with dates or sequential numbers so the timelapse is in order.\n"
           "      e.g.  World_Backup-2025-01-01.zip,  World_Backup-2025-02-01.zip")
        li("Run \u201cBuild / update caches\u201d before a large render batch \u2014 future renders\n"
           "      will be much faster and you can then delete the original ZIPs.")
        li("Both Java Edition and Bedrock Edition world formats are supported.")
        li("You can stop any render at any time with the Stop button.")
        li("Use \u201cFrom texture pack\u2026\u201d to generate an accurate per-block colour palette\n"
           "      from the actual textures your server or client is using.")
        li("After a render, open  unknown_blocks.json  in the run folder and use\n"
           "      \u201cLoad unknowns from run\u2026\u201d to find and colour any grey blocks.")
        li("Detailed log output (including errors) appears in  Advanced \u203a Log.")
        i("end", "\n")
        i("end",
          f"Open this tab at any time via the Help tab above.  \u2022  {APP_ABBR} v{APP_VERSION}\n",
          "note")

        txt.configure(state="disabled")

    def _sync_custom(self):
        is_custom = self.target_var.get().strip().lower().startswith("custom")
        if is_custom:
            self.custom_wh_frame.grid()
        else:
            self.custom_wh_frame.grid_remove()

    def _update_cache_dim_state(self, *_):
        """Enable or disable the cache-dimension checkboxes.

        Checkboxes are disabled when cache mode is 'none' (nothing to build)
        or when a job is currently running.
        """
        is_none = self.cache_mode_var.get() == CACHE_MODE_NONE
        is_busy = bool(self.busy_var.get())
        state = "disabled" if (is_none or is_busy) else "normal"
        for cb in self.cache_dim_checks:
            try:
                cb.configure(state=state)
            except Exception:
                pass

    def _is_original_timelapse_target(self) -> bool:
        return self.target_var.get().strip().lower().startswith("original")

    def _sync_cache_controls_for_crop(self):
        crop_enabled = bool(self.limit_enabled_var.get())
        non_original_target = not self._is_original_timelapse_target()

        if crop_enabled:
            mode = self.cache_mode_var.get().strip() or CACHE_MODE_SURFACE
            if mode != CACHE_MODE_NONE:
                self._cache_mode_before_crop = mode
                self.cache_mode_var.set(CACHE_MODE_NONE)
            self.cache_crop_note_var.set("No cache files will be generated while crop is enabled.")
            try:
                if self.cache_mode_combo is not None:
                    self.cache_mode_combo.configure(state="disabled")
            except Exception:
                pass
            try:
                if hasattr(self, "cache_btn"):
                    self.cache_btn.configure(state="disabled")
            except Exception:
                pass
            return

        if non_original_target:
            mode = self.cache_mode_var.get().strip() or CACHE_MODE_SURFACE
            if mode != CACHE_MODE_NONE:
                self._cache_mode_before_target = mode
                self.cache_mode_var.set(CACHE_MODE_NONE)
            self.cache_crop_note_var.set("Output cache mode is disabled unless Video resolution is set to Original (no scaling).")
            if bool(self.busy_var.get()):
                return
            try:
                if self.cache_mode_combo is not None:
                    self.cache_mode_combo.configure(state="disabled")
            except Exception:
                pass
            return

        if self.cache_mode_var.get().strip() == CACHE_MODE_NONE:
            if self._cache_mode_before_target in (CACHE_MODE_SURFACE, CACHE_MODE_ALL_BLOCKS):
                self.cache_mode_var.set(self._cache_mode_before_target)
            elif self._cache_mode_before_crop in (CACHE_MODE_SURFACE, CACHE_MODE_ALL_BLOCKS):
                self.cache_mode_var.set(self._cache_mode_before_crop)
        self.cache_crop_note_var.set("")
        if bool(self.busy_var.get()):
            return
        try:
            if self.cache_mode_combo is not None:
                self.cache_mode_combo.configure(state="readonly")
        except Exception:
            pass
        try:
            if hasattr(self, "cache_btn"):
                self.cache_btn.configure(state="normal")
        except Exception:
            pass

    def _sync_single_custom(self):
        is_custom = self.single_target_var.get().strip().lower().startswith("custom")
        if is_custom:
            self.single_custom_wh.grid()
        else:
            self.single_custom_wh.grid_remove()

    def on_cache_mode_help(self):
        messagebox.showinfo(
            "Output cache mode",
            "Controls whether .wmtt4mc cache files are built alongside output frames.\n\n"
            "- surface: default; smallest cache and fastest build.\n"
            "- all_blocks: larger cache; supports re-rendering with different Y ranges without rebuilding.\n"
            "- none: do not build new caches (existing .wmtt4mc files are still used if present).",
            parent=self,
        )

    def toggle_advanced(self):
        self.advanced_open.set(not self.advanced_open.get())
        if self.advanced_open.get():
            self.adv_frame.pack(fill="x", pady=(8, 0))
        else:
            self.adv_frame.pack_forget()
        self._resize_to_fit()

    def _sync_debug_button_visibility(self):
        if self.debug_blocks_var.get():
            try:
                self.debug_btn.state(["!disabled"])
            except Exception:
                self.debug_btn.configure(state="normal")
        else:
            try:
                self.debug_btn.state(["disabled"])
            except Exception:
                self.debug_btn.configure(state="disabled")

    # ---------- File pickers ----------
    def pick_folder(self):
        d = filedialog.askdirectory(
            title="Select folder containing backups, world folders, or .wmtt4mc caches",
            initialdir=(self.folder_var.get().strip() or None),
        )
        if d:
            self.folder_var.set(d)
            self._persist_ui_settings()

    def pick_output(self):
        d = filedialog.askdirectory(
            title="Select output folder",
            initialdir=(self.out_var.get().strip() or None),
        )
        if d:
            self.out_var.set(d)
            self._persist_ui_settings()

    def open_output_folder(self):
        try:
            path = self.out_var.get().strip()
            if not path:
                return
            if sys.platform.startswith("win"):
                os.startfile(path)
            elif sys.platform == "darwin":
                os.system(f'open "{path}"')
            else:
                os.system(f'xdg-open "{path}"')
        except Exception:
            pass

    def pick_single_zip(self):
        f = filedialog.askopenfilename(
            title="Select a world backup ZIP or .wmtt4mc cache",
            filetypes=[("Snapshot sources", "*.zip *.wmtt4mc"), ("ZIP files", "*.zip"), ("Cache files", "*.wmtt4mc")],
        )
        if f:
            self.single_zip_var.set(f)

    def pick_single_out(self):
        f = filedialog.asksaveasfilename(title="Save PNG as", defaultextension=".png", filetypes=[("PNG", "*.png")])
        if f:
            self.single_out_png_var.set(f)

    # ---------- Logging / UI state ----------
    def _log(self, msg: str, which: str = "timelapse"):
        msg = normalize_log_text(str(msg))
        box = self.log_text if which == "timelapse" else self.single_log_text
        try:
            box.insert("end", str(msg).replace("\r\n", "\n").replace("\n", "\n") + "\n")
            box.see("end")
        except Exception:
            pass

    def _set_status(self, line1: str, line2: str = ""):
        self.status1_var.set(normalize_log_text(line1))
        self.status2_var.set(normalize_log_text(line2))

    def _set_progress(self, v: float):
        try:
            self.progress_var.set(float(v))
        except Exception:
            pass

    def _set_busy(self, busy: bool):
        self.busy_var.set(bool(busy))
        state = "disabled" if busy else "normal"
        try:
            self.go_btn.configure(state=state)
        except Exception:
            pass
        try:
            self.cache_btn.configure(state=state)
        except Exception:
            pass
        try:
            self.preflight_btn.configure(state=state)
        except Exception:
            pass
        if not busy:
            self._sync_cache_controls_for_crop()
        else:
            try:
                if self.cache_mode_combo is not None:
                    self.cache_mode_combo.configure(state="disabled")
            except Exception:
                pass
        self._update_cache_dim_state()

    # ---------- Actions ----------
    def on_cancel(self):
        if not (self.worker_thread and self.worker_thread.is_alive()):
            self.cancel_event.set()
            _force_stop_registered_pools()
            return

        if self.current_task == "timelapse":
            self._paused_worker_pids = self._pause_child_processes()
            choice = messagebox.askyesnocancel(
                "Stop Rendering",
                "Stop rendering?\n\n"
                "Yes: Stop now and build a GIF from completed frames.\n"
                "No: Stop immediately and do not build a GIF.\n"
                "Cancel: Continue rendering.",
                parent=self,
            )
            if choice is None:
                self._resume_child_processes(self._paused_worker_pids)
                self._paused_worker_pids = []
                self._set_status("Continuing render.", "Stop request cancelled.")
                return

            self._resume_child_processes(self._paused_worker_pids)
            self._paused_worker_pids = []
            if choice:
                self.stop_control["mode"] = "partial_gif"
                self.cancel_event.set()
                _force_stop_registered_pools(log_cb=lambda m: self._log(m, "timelapse"))
                self._set_status("Stop requested…", "Finishing current work and building a partial GIF.")
                return

            self.stop_control["mode"] = "immediate"
            self.cancel_event.set()
            _force_stop_registered_pools(log_cb=lambda m: self._log(m, "timelapse"))
            self._force_kill_child_processes()
            self._set_status("Stop requested…", "Stopping immediately. GIF build will be skipped.")
            return

        self.cancel_event.set()
        _force_stop_registered_pools()
        self._set_status("Stop requested…", "Stopping current task as quickly as possible.")

    def _pause_child_processes(self) -> List[int]:
        paused: List[int] = []
        try:
            import psutil  # type: ignore
            me = psutil.Process(os.getpid())
            for p in me.children(recursive=True):
                try:
                    if p.is_running():
                        p.suspend()
                        paused.append(int(p.pid))
                except Exception:
                    pass
            if paused:
                self._log(f"[STOP] Paused {len(paused)} worker process(es) while waiting for stop choice.", "timelapse")
        except Exception:
            paused = []
        return paused

    def _resume_child_processes(self, pids: List[int]) -> None:
        if not pids:
            return
        try:
            import psutil  # type: ignore
            resumed = 0
            for pid in pids:
                try:
                    proc = psutil.Process(int(pid))
                    proc.resume()
                    resumed += 1
                except Exception:
                    pass
            if resumed:
                self._log(f"[STOP] Resumed {resumed} paused worker process(es).", "timelapse")
        except Exception:
            pass

    def _force_kill_child_processes(self):
        """Best-effort kill of child worker processes for fast shutdown."""
        try:
            import psutil  # type: ignore
            me = psutil.Process(os.getpid())
            children = me.children(recursive=True)
            for p in children:
                try:
                    p.terminate()
                except Exception:
                    pass
            try:
                _gone, alive = psutil.wait_procs(children, timeout=2.0)
            except Exception:
                alive = []
            for p in alive:
                try:
                    p.kill()
                except Exception:
                    pass
        except Exception:
            pass

    def _close_when_worker_stops(self):
        if self.worker_thread and self.worker_thread.is_alive():
            if time.time() >= self._close_deadline:
                _force_stop_registered_pools()
                self._force_kill_child_processes()
                try:
                    self.destroy()
                except Exception:
                    pass
                return
            self.after(150, self._close_when_worker_stops)
            return
        try:
            self.destroy()
        except Exception:
            pass

    def on_close(self):
        if not (self.worker_thread and self.worker_thread.is_alive()):
            self.destroy()
            return

        if self.current_task == "timelapse":
            self._paused_worker_pids = self._pause_child_processes()
            choice = messagebox.askyesnocancel(
                "Close While Rendering",
                "A render is still running.\n\n"
                "Yes: Stop and build a partial GIF, then close.\n"
                "No: Stop immediately (no GIF), then close.\n"
                "Cancel: Keep rendering.",
                parent=self,
            )
            if choice is None:
                self._resume_child_processes(self._paused_worker_pids)
                self._paused_worker_pids = []
                return
            self._resume_child_processes(self._paused_worker_pids)
            self._paused_worker_pids = []
            self.stop_control["mode"] = "partial_gif" if choice else "immediate"
        else:
            if not messagebox.askyesno(
                "Close While Busy",
                "A background task is still running. Stop it and close the app?",
                parent=self,
            ):
                return
            self.stop_control["mode"] = "immediate"

        self.cancel_event.set()
        _force_stop_registered_pools(log_cb=lambda m: self._log(m, "timelapse"))
        if self.stop_control.get("mode") == "immediate":
            self._force_kill_child_processes()
        self._close_pending = True
        self._close_deadline = time.time() + 8.0
        self._set_status("Stopping…", "Closing app after workers stop.")
        self._close_when_worker_stops()

    def _gather_options(self) -> RenderOptions:
        opt = RenderOptions()
        opt.dimension = self.dimension_var.get()
        opt.y_min = int(self.ymin_var.get())
        opt.y_max = int(self.ymax_var.get())
        opt.skip_water = bool(self.skip_water_var.get())
        opt.hillshade_mode = str(self.hillshade_var.get())
        # Target preset
        tgt = self.target_var.get()
        if tgt.strip().lower().startswith("custom"):
            opt.target_preset = f"Custom ({int(self.custom_w_var.get())}x{int(self.custom_h_var.get())})"
        else:
            opt.target_preset = tgt

        # Performance controls are fully automatic and machine-adaptive.
        opt.auto_tune = True
        opt.workers = 0
        opt.fast_scan = False
        opt.aggressive_mode = False
        opt.debug_block_samples = bool(self.debug_blocks_var.get())

        opt.limit_enabled = bool(self.limit_enabled_var.get())
        opt.x_min = int(self.xmin_var.get())
        opt.z_min = int(self.zmin_var.get())
        opt.x_max = int(self.xmax_var.get())
        opt.z_max = int(self.zmax_var.get())
        opt.use_editor_palette = bool(self.use_editor_palette_var.get())
        if opt.use_editor_palette and self._pal.palette:
            opt.editor_palette = dict(self._pal.palette)
        return opt

    def _validate_timelapse_inputs(self) -> Optional[str]:
        folder = self.folder_var.get().strip()
        if not folder or not os.path.isdir(folder):
            return "Please select a folder containing backups or .wmtt4mc cache files."
        out = self.out_var.get().strip()
        if not out:
            return "Please select an output folder."
        try:
            os.makedirs(out, exist_ok=True)
        except Exception:
            return "Could not create/access output folder."
        sources = find_snapshot_sources(folder, dimension=self.dimension_var.get())
        if not sources:
            return "No supported backup or cache files were found in the selected folder."
        try:
            spf = float(self.seconds_per_frame_var.get())
            if spf <= 0:
                return "Seconds per frame must be greater than 0."
        except Exception:
            return "Seconds per frame must be a number."
        return None

    def on_run(self):
        err = self._validate_timelapse_inputs()
        if err:
            messagebox.showerror("Missing/invalid input", err)
            return

        folder = self.folder_var.get().strip()
        out_dir = self.out_var.get().strip()
        snapshots = find_snapshot_sources(folder, dimension=self.dimension_var.get())  # always includes caches, ZIPs, and world folders
        snapshots = self._prompt_cache_conflicts(snapshots)
        if snapshots is None:
            return
        opt = self._gather_options()

        self._persist_ui_settings()

        seconds_per_frame = float(self.seconds_per_frame_var.get())

        # Clear cancel + log
        self.cancel_event.clear()
        self.current_task = "timelapse"
        self.stop_control["mode"] = "partial_gif"
        self._set_busy(True)
        self._set_progress(0.0)
        self._set_status("Starting…", "")
        self._log("-" * 60, "timelapse")
        
        # Show cache discovery diagnostics
        self._log(f"Snapshots: {len(snapshots)} (processing newest → oldest)", "timelapse")
        # Use enhanced diagnostics to show what was found and persist the same lines into run.log.
        discovery_lines: List[str] = []

        def _diag_log(msg: str) -> None:
            text = str(msg)
            discovery_lines.append(text)
            self._log(text, "timelapse")

        _, _diag = discover_with_diagnostics(folder, log_cb=_diag_log, dimension=self.dimension_var.get())
        self._log(f"Output cache mode: {self.cache_mode_var.get()}", "timelapse")
        
        self._log("Note: .wmtt4mc sidecar caches are only created by 'Build / update caches'.", "timelapse")
        for snapshot in snapshots:
            if snapshot.warning:
                self._log(f"  ⚠ {snapshot.warning}", "timelapse")

        # Start worker
        # --- Crop validation ---
        if not self.validate_crop_area(
            self.xmin_var.get(), self.xmax_var.get(), self.zmin_var.get(), self.zmax_var.get(),
            self.limit_enabled_var.get(), self.target_var.get(), parent=self
        ):
            self._set_busy(False)
            self._set_status("Render cancelled.", "Invalid crop area.")
            return

        def runner():
            worker_run(
                snapshots,
                out_dir,
                opt,
                seconds_per_frame,
                self.msgq,
                self.cancel_event,
                stop_control=self.stop_control,
                input_folder=folder,
                output_cache_mode=self.cache_mode_var.get(),
                discovery_lines=discovery_lines,
            )

        self.worker_thread = threading.Thread(target=runner, daemon=True)
        self.worker_thread.start()

    def _pick_cache_dimensions(self) -> "Optional[List[str]]":
        """Show a dialog to pick which dimensions to build caches for.

        Pre-selects the current timelapse dimension.
        Returns a list of selected dimension IDs, or ``None`` if the user cancels.
        """
        _DIM_OPTIONS = [
            ("minecraft:overworld", "Overworld"),
            ("minecraft:the_nether", "Nether"),
            ("minecraft:the_end", "The End"),
        ]

        result: List[str] = []
        default_dim = (self.dimension_var.get() or "minecraft:overworld").strip()

        dlg = tk.Toplevel(self)
        dlg.title("Select dimensions to cache")
        dlg.resizable(False, False)
        dlg.grab_set()

        tk.Label(
            dlg,
            text="Build cache files for which dimension(s)?",
            anchor="w",
        ).pack(padx=20, pady=(16, 4), anchor="w")
        tk.Label(
            dlg,
            text="Each dimension is stored in its own sidecar file\n"
                 "(e.g. World_overworld.wmtt4mc, World_nether.wmtt4mc).",
            anchor="w",
            justify="left",
        ).pack(padx=20, pady=(0, 8), anchor="w")

        dim_vars: List[Tuple[str, tk.BooleanVar]] = []
        for dim_id, dim_label in _DIM_OPTIONS:
            v = tk.BooleanVar(value=(dim_id == default_dim))
            dim_vars.append((dim_id, v))
            ttk.Checkbutton(dlg, text=dim_label, variable=v).pack(padx=32, pady=3, anchor="w")

        tk.Label(
            dlg,
            text="Tip: most worlds only need Overworld.",
            anchor="w",
            font=("TkDefaultFont", 8),
        ).pack(padx=20, pady=(6, 0), anchor="w")

        def on_ok():
            selected = [dim_id for dim_id, v in dim_vars if v.get()]
            if not selected:
                messagebox.showwarning(
                    "No dimension selected",
                    "Please select at least one dimension.",
                    parent=dlg,
                )
                return
            result.extend(selected)
            dlg.destroy()

        def on_cancel():
            dlg.destroy()

        btn_frame = ttk.Frame(dlg)
        btn_frame.pack(pady=(12, 16), padx=20, anchor="e")
        ttk.Button(btn_frame, text="Build Caches", command=on_ok).pack(side="left", padx=(0, 8))
        ttk.Button(btn_frame, text="Cancel", command=on_cancel).pack(side="left")

        dlg.wait_window()
        return result if result else None

    def on_build_caches(self):
        if bool(self.limit_enabled_var.get()):
            messagebox.showinfo(
                "Crop is enabled",
                "No cache files are generated while crop is enabled.\n"
                "Disable crop to build full-world .wmtt4mc sidecar caches.",
                parent=self,
            )
            return

        folder = self.folder_var.get().strip()
        if not folder or not os.path.isdir(folder):
            messagebox.showerror("Missing input", "Please select a folder containing backups first.")
            return

        raw_sources = discover_raw_snapshot_sources(folder)
        if not raw_sources:
            messagebox.showerror("Missing input", "No ZIP backups or world folders were found to cache.")
            return

        cache_mode = self.cache_mode_var.get().strip() or CACHE_MODE_SURFACE
        if cache_mode == CACHE_MODE_NONE:
            messagebox.showinfo(
                "Cache mode is 'none'",
                "The cache mode is currently set to 'none'.\n"
                "Change it to 'surface' or 'all_blocks' in the Output section to build caches.",
                parent=self,
            )
            return
        if cache_mode == CACHE_MODE_ALL_BLOCKS:
            total_bytes = 0
            for snapshot in raw_sources:
                try:
                    if os.path.isfile(snapshot.path):
                        total_bytes += os.path.getsize(snapshot.path)
                except Exception:
                    pass
            if total_bytes >= (1024 ** 3):
                if not messagebox.askyesno(
                    "Large cache build",
                    "'all_blocks' mode can create much larger cache files and take substantially longer. Continue?",
                    parent=self,
                ):
                    return

        # Read dimension selection from UI checkboxes
        selected_dims: List[str] = []
        if self.cache_dim_overworld_var.get():
            selected_dims.append("minecraft:overworld")
        if self.cache_dim_nether_var.get():
            selected_dims.append("minecraft:the_nether")
        if self.cache_dim_end_var.get():
            selected_dims.append("minecraft:the_end")
        if not selected_dims:
            selected_dims = ["minecraft:overworld"]

        self._persist_ui_settings()
        self.cancel_event.clear()
        self.current_task = "cache"
        self.stop_control["mode"] = "immediate"
        self._set_busy(True)
        self._set_progress(0.0)
        self._set_status("Starting cache build…", "")
        self._log("-" * 60, "timelapse")
        self._log(f"Cache build candidates: {len(raw_sources)} | mode={cache_mode} | dims={', '.join(selected_dims)}", "timelapse")
        self._log("Progress bar = overall cache-build progress across all snapshots; status line also shows current-snapshot chunk progress.", "timelapse")

        def runner():
            cache_build_worker(
                raw_sources,
                cache_mode,
                selected_dims,
                int(self.ymin_var.get()),
                int(self.ymax_var.get()),
                self.msgq,
                self.cancel_event,
            )

        self.worker_thread = threading.Thread(target=runner, daemon=True)
        self.worker_thread.start()

    def on_render_single(self):
        zip_path = self.single_zip_var.get().strip()
        out_png = self.single_out_png_var.get().strip()
        if not zip_path or not os.path.isfile(zip_path):
            messagebox.showerror("Missing input", "Please select a backup ZIP or .wmtt4mc cache file.")
            return
        if not out_png:
            messagebox.showerror("Missing input", "Please choose an output PNG filename.")
            return

        self._persist_ui_settings()

        opt = RenderOptions()
        opt.dimension = self.single_dimension_var.get()
        opt.y_min = int(self.single_ymin_var.get())
        opt.y_max = int(self.single_ymax_var.get())
        opt.skip_water = bool(self.single_skip_water_var.get())
        opt.hillshade_mode = str(self.single_hillshade_var.get())

        tgt = self.single_target_var.get()
        if tgt.strip().lower().startswith("custom"):
            opt.target_preset = f"Custom ({int(self.single_custom_w_var.get())}x{int(self.single_custom_h_var.get())})"
        else:
            opt.target_preset = tgt

        opt.limit_enabled = bool(self.single_limit_enabled_var.get())
        opt.x_min = int(self.single_xmin_var.get())
        opt.z_min = int(self.single_zmin_var.get())
        opt.x_max = int(self.single_xmax_var.get())
        opt.z_max = int(self.single_zmax_var.get())
        opt.use_editor_palette = bool(self.single_use_editor_palette_var.get())
        if opt.use_editor_palette and self._pal.palette:
            opt.editor_palette = dict(self._pal.palette)

        # --- Crop validation ---
        if not self.validate_crop_area(
            self.single_xmin_var.get(), self.single_xmax_var.get(), self.single_zmin_var.get(), self.single_zmax_var.get(),
            self.single_limit_enabled_var.get(), self.single_target_var.get(), parent=self
        ):
            self._set_status("Render cancelled.", "Invalid crop area.")
            return

        # Use a worker thread and reuse message queue with "single_*" tags
        self.cancel_event.clear()
        self.current_task = "single"
        self.stop_control["mode"] = "immediate"
        self._log("-" * 60, "single")
        self._log(f"Rendering single map from: {zip_path}", "single")
        self._set_status("Rendering single map…", "")

        def single_runner():
            try:
                snapshot = snapshot_input_from_path(zip_path)
                render_snapshot_input(
                    snapshot,
                    out_png,
                    opt,
                    log_cb=lambda m: self.msgq.put(("log", m)),
                    progress_cb=lambda p, t, pct: self.msgq.put(("progress", pct * 100.0)),
                    cancel_event=self.cancel_event,
                )
                self.msgq.put(("single_done", out_png))
            except Exception:
                self.msgq.put(("single_error", traceback.format_exc()))

        self.worker_thread = threading.Thread(target=single_runner, daemon=True)
        self.worker_thread.start()

    def on_debug_one_chunk(self):
        # Existing debug worker can be reused; it expects a folder of zips typically, but we’ll just pass current folder
        folder = self.folder_var.get().strip()
        if not folder or not os.path.isdir(folder):
            messagebox.showerror("Missing input", "Select a backups folder first.")
            return
        zips = find_zip_backups(folder)
        if not zips:
            messagebox.showerror("Missing input", "No ZIP files found.")
            return

        # Pick newest by default
        default_zip = zips[0]
        # Let user pick a zip from list via file picker starting in folder
        chosen = filedialog.askopenfilename(title="Select a backup ZIP to debug",
                                            initialdir=folder,
                                            filetypes=[("ZIP files", "*.zip")])
        if chosen:
            zip_path = chosen
        else:
            zip_path = default_zip

        out_dir = self.out_var.get().strip() or os.path.join(os.path.expanduser("~"), "WMTT4MC_Output")
        os.makedirs(out_dir, exist_ok=True)

        opt = self._gather_options()
        self.cancel_event.clear()
        self.current_task = "debug"
        self.stop_control["mode"] = "immediate"
        self._set_busy(True)
        self._set_progress(0.0)
        self._set_status("Debugging…", "Rendering one chunk PNG + debug IDs.")
        self._log(f"Debug ZIP: {zip_path}", "timelapse")

        def dbg_runner():
            debug_one_chunk_worker(zip_path, out_dir, opt, self.msgq, self.cancel_event)

        self.worker_thread = threading.Thread(target=dbg_runner, daemon=True)
        self.worker_thread.start()

    def on_run_preflight(self):
        folder = self.folder_var.get().strip()
        if not folder or not os.path.isdir(folder):
            messagebox.showerror("Missing input", "Select a backups folder first.")
            return

        out_dir = self.out_var.get().strip()
        if not out_dir:
            messagebox.showerror("Missing output", "Select an output folder first.")
            return
        try:
            os.makedirs(out_dir, exist_ok=True)
        except Exception:
            messagebox.showerror("Invalid output", "Could not create/access output folder.")
            return

        opt = self._gather_options()
        self.cancel_event.clear()
        self.current_task = "preflight"
        self.stop_control["mode"] = "immediate"
        self._set_busy(True)
        self._set_progress(0.0)
        self._set_status("Running preflight report…", "Collecting input and cache diagnostics.")
        self._log("-" * 60, "timelapse")
        self._log("Preflight: no rendering will be performed.", "timelapse")

        def preflight_runner():
            preflight_report_worker(
                folder,
                out_dir,
                opt,
                self.cache_mode_var.get(),
                self.msgq,
                self.cancel_event,
            )

        self.worker_thread = threading.Thread(target=preflight_runner, daemon=True)
        self.worker_thread.start()

    def _poll_messages(self):
        try:
            while True:
                kind, payload = self.msgq.get_nowait()
                if kind == "log":
                    self._log(payload, "timelapse")
                elif kind == "status":
                    a, b = payload
                    self._set_status(a, b)
                elif kind == "progress":
                    self._set_progress(payload)
                elif kind == "done":
                    self._set_busy(False)
                    self.current_task = None
                    if payload.get("cancelled"):
                        mode = str(payload.get("stopped_mode", "")).strip().lower()
                        if mode == "immediate":
                            self._set_status("Stopped.", "Render stopped immediately. GIF was not built.")
                        else:
                            self._set_status("Stopped.", "Render stopped. Partial GIF mode selected.")
                    else:
                        self._set_status("Done.", "Timelapse finished.")
                    # Optionally delete frames if user chose
                    try:
                        run_dir = payload.get("run_dir", "")
                        frames_dir = os.path.join(run_dir, "frames")
                        if run_dir and os.path.isdir(frames_dir) and (not self.keep_frames_var.get()):
                            shutil.rmtree(frames_dir, ignore_errors=True)
                            self._log("Deleted frame PNGs (Keep frames unchecked).", "timelapse")
                    except Exception:
                        pass
                elif kind == "error":
                    self._set_busy(False)
                    self.current_task = None
                    self._set_status("Error.", "See log for details.")
                    self._log(payload, "timelapse")
                elif kind == "done_debug":
                    self._set_busy(False)
                    self.current_task = None
                    self._set_status("Debug done.", "")
                    self._log(payload, "timelapse")
                elif kind == "cache_done":
                    self._set_busy(False)
                    self.current_task = None
                    if payload.get("cancelled"):
                        self._set_status("Cache build cancelled.", "")
                    else:
                        self._set_status(
                            "Cache build finished.",
                            f"Built {payload.get('built', 0)}, skipped {payload.get('skipped', 0)}, failed {payload.get('failed', 0)}"
                        )
                        self._log(
                            f"Cache build complete: built={payload.get('built', 0)} skipped={payload.get('skipped', 0)} failed={payload.get('failed', 0)} total={payload.get('total', 0)} mode={payload.get('mode', '')}",
                            "timelapse",
                        )
                elif kind == "done_preflight":
                    self._set_busy(False)
                    self.current_task = None
                    if payload.get("cancelled"):
                        self._set_status("Preflight cancelled.", "")
                    else:
                        self._set_status(
                            "Preflight complete.",
                            f"Planned {payload.get('planned_total', 0)} item(s): {payload.get('planned_cache', 0)} cache, {payload.get('planned_raw', 0)} raw"
                        )
                        self._log(
                            f"Preflight report complete: {payload.get('report_txt', '')}",
                            "timelapse",
                        )
                        self._log(
                            f"Preflight JSON: {payload.get('report_json', '')}",
                            "timelapse",
                        )
                elif kind == "single_done":
                    self._set_busy(False)
                    self.current_task = None
                    self._set_status("Single map saved.", payload)
                    self._log(f"Saved: {payload}", "single")
                elif kind == "single_error":
                    self._set_busy(False)
                    self.current_task = None
                    self._set_status("Error.", "Single map render failed.")
                    self._log(payload, "single")
                else:
                    # Unknown message type
                    pass
        except queue.Empty:
            pass
        self.after(100, self._poll_messages)

    def main(self):
        self.after(5000, self._heartbeat)
        self.mainloop()


def main():
    palette_path = os.path.join(_app_dir(), "palette.json")
    ensure_palette_json(palette_path)
    apply_palette_overrides(palette_path)
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()