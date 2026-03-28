import os
from collections import Counter
import re
import sys
import time
import zipfile
import shutil
import tempfile
import traceback

import time

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
            time.sleep(0.15 * (i + 1))
    # fallback: try direct save; raise if it fails
    if last_err and log:
        log(f"PNG save falling back to direct write for {out_path} after retries: {last_err}")
    img.save(out_path, format="PNG")
import threading
import queue
from dataclasses import dataclass
from typing import List, Tuple, Optional, Any, Callable, Dict
from collections import deque, Counter
from concurrent.futures import ThreadPoolExecutor, as_completed


def _raw_block_id(block) -> str:
    """Return a detail-preserving string for a block.

    Prefer Amulet's full blockstate representations (these include properties like
    wood_type/color/etc when available), then fall back to str(block).
    """
    for attr in ("full_blockstate", "snbt_blockstate", "blockstate"):
        try:
            v = getattr(block, attr, None)
            if v:
                s = str(v)
                if s:
                    return s
        except Exception:
            pass

    try:
        s = str(block)
        if s:
            return s
    except Exception:
        pass
    for attr in ("namespaced_name", "base_name"):
        try:
            v = getattr(block, attr, None)
            if v:
                return str(v)
        except Exception:
            pass
    return repr(block)


import numpy as np
from PIL import Image

import tkinter as tk
from tkinter import filedialog, messagebox, ttk


class ScrollableFrame(ttk.Frame):
    """A vertically scrollable frame (Canvas + interior Frame)."""

    def __init__(self, parent, *args, **kwargs):
        super().__init__(parent, *args, **kwargs)

        self._canvas = tk.Canvas(self, highlightthickness=0)
        self._vsb = ttk.Scrollbar(self, orient="vertical", command=self._canvas.yview)
        self._canvas.configure(yscrollcommand=self._vsb.set)

        self._vsb.pack(side="right", fill="y")
        self._canvas.pack(side="left", fill="both", expand=True)

        self.interior = ttk.Frame(self._canvas)
        self._window_id = self._canvas.create_window((0, 0), window=self.interior, anchor="nw")

        def _on_configure_interior(_event):
            self._canvas.configure(scrollregion=self._canvas.bbox("all"))

        def _on_configure_canvas(event):
            self._canvas.itemconfigure(self._window_id, width=event.width)

        self.interior.bind("<Configure>", _on_configure_interior)
        self._canvas.bind("<Configure>", _on_configure_canvas)

        # Mousewheel support
        self._canvas.bind_all("<MouseWheel>", self._on_mousewheel, add="+")
        self._canvas.bind_all("<Button-4>", self._on_mousewheel, add="+")
        self._canvas.bind_all("<Button-5>", self._on_mousewheel, add="+")

    def _on_mousewheel(self, event):
        if event.num == 4:
            delta = -1
        elif event.num == 5:
            delta = 1
        else:
            delta = -1 * int(event.delta / 120) if getattr(event, "delta", 0) else 0
        if delta:
            self._canvas.yview_scroll(delta, "units")

import amulet


# =============================================================================
# Persistent user preferences (remember folders between versions)
# =============================================================================

APP_ID = "WMTT4MC"


def _get_config_path() -> str:
    """Return a per-user config path that survives app upgrades.

    Windows: %APPDATA%\WMTT4MC\config.json
    macOS/Linux: ~/.config/WMTT4MC/config.json
    """
    if sys.platform.startswith("win"):
        base = os.environ.get("APPDATA") or os.path.expanduser("~\\AppData\\Roaming")
    else:
        base = os.path.expanduser("~/.config")
    return os.path.join(base, APP_ID, "config.json")


def _load_config() -> dict:
    path = _get_config_path()
    try:
        import json

        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f) or {}
    except Exception:
        pass
    return {}


def _save_config(cfg: dict) -> None:
    path = _get_config_path()
    try:
        import json

        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2)
        os.replace(tmp, path)
    except Exception:
        # Preferences are best-effort; never crash the app.
        pass


# =============================================================================
# App identity (versioning)
# =============================================================================

APP_NAME = "World Map Timeline Tool for Minecraft"
APP_VERSION = "1.5.2ad"
APP_BUILD = "2026.01.20.2"
APP_ABBR = "WMTT4MC"
DISCLAIMER_TEXT = "NOT AN OFFICIAL MINECRAFT PRODUCT. Not affiliated with or endorsed by Mojang or Microsoft."


# =============================================================================
# Palette / block classification
# =============================================================================

PALETTE = {
    "minecraft:pale_oak_planks": (228, 218, 216),
    "minecraft:cherry_planks": (227, 179, 173),
    "minecraft:wheat": (167, 152, 73),
    "minecraft:dirt_path": (148, 122, 65),
    # Ground / vegetation
    "minecraft:grass_block": (106, 170, 64),
    "minecraft:grass": (106, 170, 64),
    "minecraft:short_grass": (106, 170, 64),
    "minecraft:tall_grass": (106, 170, 64),
    "minecraft:fern": (96, 160, 64),
    "minecraft:large_fern": (88, 150, 62),
    "minecraft:moss_block": (90, 160, 75),
    "minecraft:moss_carpet": (90, 160, 75),
    "minecraft:azalea_leaves": (78, 154, 56),
    "minecraft:flowering_azalea_leaves": (86, 160, 60),

    # Dirt family
    "minecraft:dirt": (134, 96, 67),
    "minecraft:coarse_dirt": (120, 85, 60),
    "minecraft:rooted_dirt": (120, 85, 60),
    "minecraft:podzol": (122, 102, 62),
    "minecraft:mycelium": (120, 90, 120),
    "minecraft:mud": (90, 74, 62),
    "minecraft:packed_mud": (142, 107, 80),
    "minecraft:mud_bricks": (137, 104, 79),

    # Stone family
    "minecraft:stone": (125, 125, 125),
    "minecraft:cobblestone": (120, 120, 120),
    "minecraft:granite": (149, 103, 85),
    "minecraft:diorite": (188, 188, 188),
    "minecraft:andesite": (136, 136, 136),
    "minecraft:deepslate": (80, 80, 90),
    "minecraft:tuff": (110, 110, 120),
    "minecraft:calcite": (220, 220, 225),
    "minecraft:dripstone_block": (160, 140, 120),

    # Desert / gravel
    "minecraft:sand": (219, 211, 160),
    "minecraft:red_sand": (201, 114, 48),
    "minecraft:gravel": (136, 126, 126),

    # Liquids
    "minecraft:water": (64, 64, 255),
    "minecraft:ice": (170, 210, 255),
    "minecraft:packed_ice": (140, 190, 255),
    "minecraft:blue_ice": (120, 170, 255),
    "minecraft:lava": (255, 80, 0),

    # Leaves (common; others handled by heuristic)
    "minecraft:oak_leaves": (72, 144, 48),
    "minecraft:spruce_leaves": (48, 120, 48),
    "minecraft:birch_leaves": (96, 168, 64),
    "minecraft:jungle_leaves": (64, 140, 48),
    "minecraft:acacia_leaves": (84, 148, 52),
    "minecraft:dark_oak_leaves": (48, 96, 48),
    "minecraft:mangrove_leaves": (66, 132, 54),
    "minecraft:cherry_leaves": (229, 173, 194),

    # Logs (common; others handled by heuristic)
    "minecraft:oak_log": (102, 81, 51),
    "minecraft:spruce_log": (80, 60, 40),
    "minecraft:birch_log": (200, 190, 150),
    "minecraft:jungle_log": (120, 90, 60),
    "minecraft:acacia_log": (140, 90, 60),
    "minecraft:dark_oak_log": (60, 45, 30),
    "minecraft:mangrove_log": (110, 70, 55),
    "minecraft:cherry_log": (170, 130, 120),

    # Snow / Nether / End
    "minecraft:snow": (240, 240, 240),
    "minecraft:snow_block": (240, 240, 240),
    "minecraft:netherrack": (110, 54, 52),
    "minecraft:end_stone": (220, 220, 180),
    "minecraft:obsidian": (20, 18, 30),

    # Other
    "minecraft:clay": (160, 170, 180),
    "minecraft:terracotta": (152, 94, 68),

    # Ores / minerals
    "minecraft:coal_ore": (50, 50, 50),
    "minecraft:iron_ore": (155, 135, 120),
    "minecraft:copper_ore": (170, 105, 80),
    "minecraft:gold_ore": (205, 180, 70),
    "minecraft:redstone_ore": (170, 55, 55),
    "minecraft:lapis_ore": (55, 85, 170),
    "minecraft:emerald_ore": (55, 170, 85),
    "minecraft:diamond_ore": (90, 190, 190),
    "minecraft:nether_gold_ore": (195, 155, 85),
    "minecraft:nether_quartz_ore": (185, 170, 165),
    "minecraft:ancient_debris": (110, 80, 70),

    # Common utility blocks / details
    "minecraft:torch": (245, 200, 80),
    "minecraft:wall_torch": (245, 200, 80),
    "minecraft:redstone_torch": (200, 60, 60),
    "minecraft:chest": (150, 110, 65),
    "minecraft:barrel": (130, 95, 60),
    "minecraft:furnace": (110, 110, 110),
    "minecraft:crafting_table": (140, 105, 70),
    "minecraft:scaffolding": (190, 170, 120),
    "minecraft:rail": (130, 130, 130),
    "minecraft:powered_rail": (150, 130, 90),
    "minecraft:detector_rail": (150, 130, 90),
    "minecraft:activator_rail": (150, 130, 90),

    # Shapes (defaults; variants handled elsewhere when available)
    "minecraft:slab": (140, 130, 120),
    "minecraft:stairs": (140, 130, 120),
    "minecraft:trapdoor": (150, 120, 80),


    # Aliases / common variants
    "minecraft:coal_block": (18, 18, 18),
    "minecraft:polished_blackstone": (43, 43, 46),
    "minecraft:blackstone": (35, 35, 38),
    "minecraft:blackstone_wall": (38, 38, 40),
    # Planks (common)
    "minecraft:oak_planks": (162, 130, 78),
    "minecraft:spruce_planks": (114, 84, 48),
    "minecraft:birch_planks": (196, 176, 118),
    "minecraft:jungle_planks": (160, 115, 80),
    "minecraft:acacia_planks": (170, 90, 52),
    "minecraft:dark_oak_planks": (70, 53, 33),
    "minecraft:mangrove_planks": (110, 55, 50),
    "minecraft:bamboo_planks": (204, 192, 112),
    "minecraft:crimson_planks": (122, 55, 88),
    "minecraft:warped_planks": (50, 110, 110),
    # Logs / woods (top colors are approximate averages)
    "minecraft:bamboo_block": (166, 160, 96),

}


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
        from pathlib import Path
        import json

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
    hillshade: bool = True

    target_preset: str = "1080p (1920x1080)"
    workers: int = 3
    fast_scan: bool = False
    aggressive_mode: bool = False

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


class CancelledError(Exception):
    pass




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
        import json
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
                # keep going even if one entry is malformed
                continue

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


def classify_block(block) -> Tuple[Tuple[int, int, int], str, bool, str]:
    """
    Returns: (rgb, key_used, is_known, reason)
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
    if palette_key in PALETTE:
        return PALETTE[palette_key], palette_key, True, "palette"
    if bid in PALETTE:
        return PALETTE[bid], bid, True, "palette"

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
    if "water" in bid or bid.endswith(":seagrass") or "kelp" in bid:
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



def compute_hillshade(height: np.ndarray) -> np.ndarray:
    dy, dx = np.gradient(height.astype(np.float32))
    shade = -(dx * -1.0 + dy * -1.0)
    smin, smax = np.percentile(shade, 2), np.percentile(shade, 98)
    if smax - smin < 1e-6:
        return np.ones_like(height, dtype=np.float32)
    shade = (shade - smin) / (smax - smin)
    return (0.75 + 0.5 * shade).astype(np.float32)


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
) -> Tuple[bool, int, str]:
    if not fast_scan:
        for y in range(y_max, y_min - 1, -1):
            b = chunk.get_block(lx, y, lz)
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
        b = chunk.get_block(lx, y, lz)
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
        b = chunk.get_block(lx, yy, lz)
        raw = _raw_block_id(b)
        bid = normalize_block_id(raw)
        if is_skippable(bid):
            continue
        return True, yy, raw

    return False, y_min, "minecraft:air"


def render_world_map(
    world_root: str,
    out_png: str,
    opt: RenderOptions,
    log_cb: Optional[Callable[[str], None]] = None,
    progress_cb: Optional[Callable[[int, int, float], None]] = None,
    cancel_event: Optional[threading.Event] = None,
    debug_snapshot_path: Optional[str] = None,
    debug_context_header: str = "",
) -> Tuple[int, int, int, int, int, int, int, int, int]:
    world0 = None
    try:
        world0 = amulet.load_level(world_root)
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
            dim_id = resolve_dimension_id(world0, opt.dimension)
            chunk_coords = world_all_chunk_coords(world0, dim_id)
            if not chunk_coords:
                raise RuntimeError("No chunks found in this dimension (world may be empty/unexplored).")
    
            # Apply optional render limiting (block X/Z rectangle). This greatly speeds up rendering
            # and focuses on the important area.
            if opt.limit_enabled:
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
                if not chunk_coords:
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
    
            processed_chunks = 0
            last_progress_emit = 0.0
    
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
    
            n_workers = max(1, min(int(opt.workers), 4))
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
    
            chunk_access_lock = None
            try:
                lw = getattr(world0, 'level_wrapper', None)
                if lw is not None and ('LevelDB' in type(lw).__name__ or 'leveldb' in str(type(lw)).lower()):
                    # Some Bedrock worlds are happiest when the DB read handle is serialized.
                    # We still parallelize the expensive per-column scan.
                    chunk_access_lock = threading.Lock()
            except Exception:
                chunk_access_lock = None
    
            def worker_fn(worker_id: int, coords: List[Tuple[int, int]]):
                nonlocal chunks_rendered, chunks_skipped, colored_cols, air_only_cols, unknown_cols
    
                if cancel_event is not None and cancel_event.is_set():
                    return
    
                local_colored = 0
                local_air = 0
                local_unknown = 0
                local_skipped = 0
                local_rendered = 0
                local_unknown_counts = Counter()
                local_exact_key_miss_counts = Counter()
                local_base_id_miss_counts = Counter()
                local_unknown_raw_counts = Counter()
    
                for (cx, cz) in coords:
                    if cancel_event is not None and cancel_event.is_set():
                        break
    
                    try:
                        if chunk_access_lock is None:
                            ch = get_chunk_fn_shared(cx, cz)
                        else:
                            with chunk_access_lock:
                                ch = get_chunk_fn_shared(cx, cz)
                    except Exception:
                        local_skipped += 1
                        with counters_lock:
                            nonlocal processed_chunks
                            processed_chunks += 1
                        continue
    
                    if ch is None:
                        local_skipped += 1
                        with counters_lock:
                            processed_chunks += 1
                        continue
    
                    local_rendered += 1
    
                    base_x = cx * 16
                    base_z = cz * 16
    
                    xs = iter_sample_positions(base_x, 16, min_x)
                    zs = iter_sample_positions(base_z, 16, min_z)
    
                    for wx in xs:
                        if cancel_event is not None and cancel_event.is_set():
                            break
                        if wx < min_x or wx > max_x:
                            continue
                        ix = (wx - min_x) // bpp
                        lx = wx - base_x
    
                        for wz in zs:
                            if cancel_event is not None and cancel_event.is_set():
                                break
                            if wz < min_z or wz > max_z:
                                continue
                            iz = (wz - min_z) // bpp
                            lz = wz - base_z
    
                            found, top_y, raw_block = find_top_block_in_column(
                                ch, int(lx), int(lz),
                                opt.y_min, opt.y_max,
                                opt.skip_water,
                                opt.fast_scan,
                            )
    
                            if found:
                                # raw_block may be a string id or an Amulet block object
                                raw_str = str(raw_block)
                                rgb_px, norm_id, is_known, _ = classify_block(raw_str)
                                local_colored += 1
                                rgb[iz, ix, :] = rgb_px
                                hmap[iz, ix] = top_y
    
                                if opt.debug_block_samples:
                                    with counters_lock:
                                        if raw_str not in samples_set and len(samples_set) < max_samples:
                                            samples_set.add(raw_str)
                                            samples_raw.append(raw_str)
    
                                if (not is_known) and opt.debug_block_samples:
                                    local_unknown += 1
                                    local_unknown_counts[norm_id] += 1
                                    _base = norm_id.split('[', 1)[0].split('{', 1)[0]
                                    if _base in PALETTE:
                                        local_exact_key_miss_counts[_base] += 1
                                    else:
                                        local_base_id_miss_counts[_base] += 1
                                    local_unknown_raw_counts[raw_str] += 1
                            else:
                                local_air += 1
                                rgb[iz, ix, :] = (0, 0, 0)
                                hmap[iz, ix] = opt.y_min
    
                    if not opt.aggressive_mode:
                        time.sleep(0.0005)
    
                    with counters_lock:
                        processed_chunks += 1
    
                    if debug_enabled:
                        now = time.time()
                        if (not debug_written_once) and (now - render_start) >= DEBUG_FORCE_FIRST_WRITE_AFTER_SEC:
                            with counters_lock:
                                maybe_write_debug_snapshot(force=True)
                        else:
                            with counters_lock:
                                maybe_write_debug_snapshot(force=False)
    
                    if progress_cb is not None:
                        now = time.time()
                        with progress_lock:
                            nonlocal last_progress_emit
                            if now - last_progress_emit >= 0.4:
                                pct = processed_chunks / max(1, chunks_total)
                                progress_cb(processed_chunks, chunks_total, pct)
                                last_progress_emit = now
    
                with counters_lock:
                    chunks_rendered += local_rendered
                    chunks_skipped += local_skipped
                    colored_cols += local_colored
                    air_only_cols += local_air
                    unknown_cols += local_unknown
                    unknown_norm_counts.update(local_unknown_counts)
                    unknown_raw_counts.update(local_unknown_raw_counts)
                    exact_key_miss_counts.update(local_exact_key_miss_counts)
                    base_id_miss_counts.update(local_base_id_miss_counts)
    
            with ThreadPoolExecutor(max_workers=n_workers) as ex:
                futures = [ex.submit(worker_fn, i, part) for i, part in enumerate(parts)]
                for fut in as_completed(futures):
                    if cancel_event is not None and cancel_event.is_set():
                        break
                    fut.result()
    
            if cancel_event is not None and cancel_event.is_set():
                maybe_write_debug_snapshot(force=True)
                raise CancelledError("Cancelled during rendering.")
    
            maybe_write_debug_snapshot(force=True)
    
            if chunks_rendered == 0:
                raise RuntimeError("No chunks could be loaded (all chunk fetches failed).")
    
            if progress_cb is not None:
                progress_cb(chunks_total, chunks_total, 1.0)
    
            if opt.hillshade:
                shade = compute_hillshade(hmap)
                rgb_f = rgb.astype(np.float32) * shade[..., None]
                rgb = np.clip(rgb_f, 0, 255).astype(np.uint8)
    
            img = Image.fromarray(rgb, mode="RGB")
            img = fit_to_target(img, target)
            img.save(out_png)
    
            if log_cb and debug_enabled:
                log_cb(f"  Debug snapshot file: {debug_snapshot_path}")
                log_cb(f"  Column scan results: colored={colored_cols} air_only={air_only_cols} unknown_colored={unknown_cols}")
                if unknown_cols > 0:
                    top = unknown_norm_counts.most_common(10)
                    log_cb("  Top unknown normalized IDs (rendered gray):")
                    for bid, cnt in top:
                        log_cb(f"    {cnt:8d}  {bid}")
    
            return (min_x, max_x, min_z, max_z, chunks_rendered, chunks_skipped, colored_cols, air_only_cols, bpp)
    
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
                    rgb_px, norm_id, is_known, _ = classify_block(raw_id)
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

        if opt.hillshade:
            shade = compute_hillshade(hmap)
            rgb_f = rgb.astype(np.float32) * shade[..., None]
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


def sort_frames_chronological(frames: List[Tuple[int, str]]) -> List[str]:
    frames_sorted = sorted(frames, key=lambda t: t[0])
    return [p for _, p in frames_sorted]


def build_gif(frame_paths: List[str], out_gif: str, seconds_per_frame: float, loop: int = 0):
    if not frame_paths:
        raise ValueError("No frames to animate.")
    frames = [Image.open(p).convert("RGB") for p in frame_paths]
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


def worker_run(zips: List[str],
    out_dir: str,
    opt: RenderOptions,
    seconds_per_frame: float,msgq: "queue.Queue[tuple]",
    cancel_event: threading.Event,
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

        log(f"{APP_NAME} v{APP_VERSION} (build {APP_BUILD})")
        log(f"Run folder: {run_dir}")
        log(f"Log file: {run_log_path}")
        log(f"Found {len(zips)} backups.")
        log(f"Output: {out_dir}")
        log(f"Dimension: {opt.dimension} | y=[{opt.y_min},{opt.y_max}] | seconds_per_frame={seconds_per_frame}")
        log(f"Target: {opt.target_preset} | workers={opt.workers} | fast_scan={opt.fast_scan} | aggressive={opt.aggressive_mode}")
        log("Processing order: newest backups first (reverse filename index).")
        log("GIF order: oldest → newest (chronological).")
        log("-" * 60)

        rendered_frames: List[Tuple[int, str]] = []
        skipped: List[Tuple[str, str]] = []

        zip_times: List[float] = []
        total_zips = len(zips)
        chronological = list(reversed(zips))  # oldest -> newest
        frame_no_by_zip = {zp: i+1 for i, zp in enumerate(chronological)}
        RENDER_WEIGHT = 92.0
        overall_eta_smoother = EtaSmoother(window=5)

        for zip_i, zip_path in enumerate(zips, start=1):
            if cancel_event and cancel_event.is_set():
                log("-" * 60)
                log("Cancelled.")
                break

            name = os.path.basename(zip_path)
            idx = extract_index_from_name(name)

            debug_snapshot_path = os.path.join(run_dir, f"debug_block_ids_{zip_i:03d}_{safe_filename(name)}.txt")

            log(f"[{zip_i}/{total_zips}] {name}")
            log(f"  Debug snapshot path (if enabled): {debug_snapshot_path}")

            t0 = time.time()
            frame_no = frame_no_by_zip.get(zip_path, zip_i)
            frame_png = os.path.join(frames_dir, f"frame_{frame_no:04d}.png")

            frame_eta_smoother = EtaSmoother(window=5)
            last_frame_eta: Optional[float] = None
            last_total_eta: Optional[float] = None
            last_emit_time = 0.0

            def per_chunk_progress(chunks_done: int, chunks_total: int, frac_done: float):
                nonlocal last_frame_eta, last_total_eta, last_emit_time

                now = time.time()
                elapsed = now - t0

                rem = estimate_remaining_seconds(elapsed, frac_done)
                last_frame_eta = frame_eta_smoother.add(rem if rem is not None else -1.0) or last_frame_eta

                remaining_frames_after_this = max(0, total_zips - zip_i)

                if last_frame_eta is not None:
                    est_total_this_frame = elapsed + last_frame_eta
                    rough_total_remaining = last_frame_eta + (est_total_this_frame * remaining_frames_after_this)
                    last_total_eta = overall_eta_smoother.add(rough_total_remaining) or last_total_eta

                overall = ((zip_i - 1) / total_zips) * RENDER_WEIGHT + frac_done * (RENDER_WEIGHT / total_zips)
                progress(overall)

                if now - last_emit_time < 0.35:
                    return
                last_emit_time = now

                pct = frac_done * 100.0
                line1 = f"[{zip_i}/{total_zips}] {name} | {pct:5.1f}% (chunks {chunks_done}/{chunks_total})"
                if last_frame_eta is not None:
                    line1 += f" | Frame ETA: {fmt_seconds(last_frame_eta)}"

                line2 = ""
                if last_total_eta is not None:
                    line2 = f"Rough estimate on total render time remaining: {fmt_seconds(last_total_eta)}"

                status(line1, line2)

            try:
                with tempfile.TemporaryDirectory() as tmp:
                    extract_root, candidates = unzip_world_find_roots(zip_path, tmp)

                    if not candidates:
                        raise RuntimeError(
                            f"No level.dat found after extraction (not a full world backup). Extracted to: {extract_root}"
                        )

                    loaded = False
                    last_error: Optional[Exception] = None
                    bounds = None

                    status(f"[{zip_i}/{total_zips}] Loading world: {name}", "")

                    for world_root in candidates:
                        try:
                            header = (
                                f"Backup: {name}\\n"
                                f"ZIP path: {zip_path}\\n"
                                f"Candidate world root: {world_root}\\n"
                                f"Dimension: {opt.dimension}\\n"
                                f"Y range: [{opt.y_min},{opt.y_max}]\\n"
                                f"Target: {opt.target_preset}\\n"
                                f"Workers: {opt.workers}\\n"
                                f"Fast scan: {opt.fast_scan}\\n"
                                f"Aggressive mode: {opt.aggressive_mode}\\n"
                                f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}\\n"
                                f"NOTE: Unknown IDs are rendered gray and listed in this file.\\n"
                            )
                            bounds = render_world_map(
                                world_root,
                                frame_png,
                                opt,
                                log_cb=log,
                                progress_cb=per_chunk_progress,
                                cancel_event=cancel_event,
                                debug_snapshot_path=debug_snapshot_path,
                                debug_context_header=header,
                            )
                            loaded = True
                            break
                        except CancelledError:
                            # Keep any partially/fully rendered frame PNGs.
                            # If the user cancels, we should not delete already-produced outputs.
                            log("  -> CANCELLED during frame render.")
                            log(f"  (If Debug block IDs was enabled, see: {debug_snapshot_path})")
                            msgq.put(("done", {
                                "run_dir": run_dir,
                                "frames_dir": frames_dir,
                                "gif": None,
                                "skipped": len(skipped),
                                "skipped_report": None,
                                "cancelled": True,
                            }))
                            run_log.close()
                            return
                        except Exception as e:
                            last_error = e

                    if not loaded:
                        raise RuntimeError(f"{type(last_error).__name__}: {last_error}") from last_error

                rendered_frames.append((frame_no, frame_png))

                (min_x, max_x, min_z, max_z,
                 chunks_rendered, chunks_skipped,
                 cols_colored, cols_air, bpp) = bounds

                log(
                    f"  -> OK: {os.path.relpath(frame_png, run_dir)} "
                    f"| bounds x[{min_x},{max_x}] z[{min_z},{max_z}] "
                    f"| chunks rendered={chunks_rendered} skipped={chunks_skipped} "
                    f"| cols colored={cols_colored} air_only={cols_air} "
                    f"| blocks_per_pixel={bpp}"
                )

            except Exception as e:
                reason = f"{type(e).__name__}: {e}"
                tb = traceback.format_exc()
                skipped.append((name, reason, tb))
                log(f"  -> SKIPPED: {reason}")
                log("  Traceback (for debugging):")
                for _line in tb.rstrip().splitlines():
                    log(f"    {_line}")
                if opt.debug_block_samples:
                    log(f"  Debug snapshot (partial, may exist): {debug_snapshot_path}")

            elapsed_zip = time.time() - t0
            zip_times.append(elapsed_zip)

            avg = sum(zip_times) / len(zip_times)
            remaining = max(0, total_zips - zip_i)
            status(
                f"Completed {zip_i}/{total_zips} | avg/backup {fmt_seconds(avg)}",
                f"Rough estimate on total render time remaining: {fmt_seconds(avg * remaining)}"
            )

        skipped_report = None
        if skipped:
            skipped_report = os.path.join(run_dir, "skipped_backups.txt")
            with open(skipped_report, "w", encoding="utf-8") as f:
                f.write("Skipped backups:\\n\\n")
                for nm, reason in skipped:
                    f.write(f"- {nm}\\n  {reason}\\n\\n")
            log("-" * 60)
            log(f"Wrote skip report: {skipped_report}")

        if not rendered_frames:
            raise RuntimeError(
                "All backups failed to render; no frames were produced.\\n"
                "Check skipped_backups.txt and run.log in the output folder for details."
            )

        status("Building animated GIF…", "")
        log("-" * 60)
        log(f"Building GIF from {len(rendered_frames)} frames…")
        progress(93.0)

        gif_frames = sort_frames_chronological(rendered_frames)
        world_name = ""
        try:
            # Attempt to infer from newest backup folder name
            world_name = os.path.basename(os.path.dirname(world_root_guess))
        except Exception:
            world_name = ""
        base = getattr(opt, "output_name", "") or (safe_filename(world_name) if world_name else "timelapse")
        if not base.lower().endswith("_wmtt4mc"):
            base = base + "_wmtt4mc"
        out_gif = os.path.join(run_dir, base + ".gif")
        build_gif(gif_frames, out_gif, seconds_per_frame=seconds_per_frame, loop=0)

        progress(100.0)
        log(f"Saved GIF: {out_gif}")
        run_log.close()

        msgq.put(("done", {
            "run_dir": run_dir,
            "frames_dir": frames_dir,
            "gif": out_gif,
            "skipped": len(skipped),
            "skipped_report": skipped_report,
            "cancelled": False,
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
        log(f"Options: dim={opt.dimension} y=[{opt.y_min},{opt.y_max}] skip_water={opt.skip_water} hillshade={opt.hillshade} fast_scan={opt.fast_scan}")
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
                f"Hillshade: {opt.hillshade}\\n"
                f"Skip water: {opt.skip_water}\\n"
                f"Fast scan: {opt.fast_scan}\\n"
                f"NOTE: Unknown IDs are rendered gray and listed below.\\n"
            )

            status(f"Debug: rendering chunk ({cx},{cz})…", "")
            progress(30.0)

            opt2 = RenderOptions(**{**opt.__dict__})
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



class App(tk.Tk):
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
# --- shared state ---
        self.msgq: "queue.Queue[tuple]" = queue.Queue()
        self.cancel_event = threading.Event()
        self.worker_thread: Optional[threading.Thread] = None

        # --- Timelapse tab vars ---
        self.folder_var = tk.StringVar()
        self.out_var = tk.StringVar(value=os.path.join(os.path.expanduser("~"), "WMTT4MC_Output"))
        self.dimension_var = tk.StringVar(value="minecraft:overworld")

        # Restore last used folders (persisted between versions)
        try:
            cfg = _load_config()
            last_in = cfg.get("last_input_dir")
            last_out = cfg.get("last_output_dir")
            if isinstance(last_in, str) and last_in and os.path.isdir(last_in):
                self.folder_var.set(last_in)
            if isinstance(last_out, str) and last_out and os.path.isdir(last_out):
                self.out_var.set(last_out)
        except Exception:
            pass

        self.target_var = tk.StringVar(value="1080p (1920x1080)")
        self.custom_w_var = tk.IntVar(value=1920)
        self.custom_h_var = tk.IntVar(value=1080)

        # Primary timing control
        self.seconds_per_frame_var = tk.DoubleVar(value=1.0)
        # Advanced control (derived)
        self.fps_var = tk.DoubleVar(value=1.0)

        self.skip_water_var = tk.BooleanVar(value=False)
        self.hillshade_var = tk.BooleanVar(value=True)
        self.ymin_var = tk.IntVar(value=0)
        self.ymax_var = tk.IntVar(value=320)

        # Crop/limit
        self.limit_enabled_var = tk.BooleanVar(value=False)
        self.xmin_var = tk.IntVar(value=0)
        self.zmin_var = tk.IntVar(value=0)
        self.xmax_var = tk.IntVar(value=0)
        self.zmax_var = tk.IntVar(value=0)

        # Output options
        self.output_name_var = tk.StringVar(value="")
        self.keep_frames_var = tk.BooleanVar(value=False)

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
        self.single_hillshade_var = tk.BooleanVar(value=True)
        self.single_ymin_var = tk.IntVar(value=0)
        self.single_ymax_var = tk.IntVar(value=320)

        # Status/progress
        self.status1_var = tk.StringVar(value="Ready.")
        self.status2_var = tk.StringVar(value="")
        self.progress_var = tk.DoubleVar(value=0.0)
        self.busy_var = tk.BooleanVar(value=False)

        self._build_ui()
        self._resize_to_fit()

        self._wire_timing_vars()

        self.after(100, self._poll_messages)

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
        nb = ttk.Notebook(root)
        nb.pack(fill="both", expand=True)

        self.tab_timelapse = ttk.Frame(nb)
        nb.add(self.tab_timelapse, text="Timelapse")

        sf1 = ScrollableFrame(self.tab_timelapse)
        sf1.pack(fill="both", expand=True)
        self._build_timelapse_tab(sf1.interior)

        # Footer: status + disclaimer
        footer = ttk.Frame(root)
        footer.pack(fill="x", side="bottom")

        ttk.Label(footer, text=DISCLAIMER_TEXT).pack(side="left", padx=8)
        ttk.Label(footer, text=f"{APP_ABBR} v{APP_VERSION} (build {APP_BUILD})").pack(side="right", padx=8)

    def _build_timelapse_tab(self, parent):
        pad = {"padx": 8, "pady": 6}

        top = ttk.Frame(parent)
        top.pack(fill="x", **pad)

        ttk.Label(top, text="Backups folder (ZIPs):").grid(row=0, column=0, sticky="w", **pad)
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
                     state="readonly", width=22).grid(row=0, column=1, sticky="w", **pad)

        ttk.Label(opts, text="Video resolution:").grid(row=0, column=2, sticky="w", **pad)
        ttk.Combobox(opts, textvariable=self.target_var,
                     values=["720p (1280x720)", "1080p (1920x1080)", "4K (3840x2160)", "Original (no scaling)", "Custom…"],
                     state="readonly", width=22).grid(row=0, column=3, sticky="w", **pad)

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
        ttk.Checkbutton(opts, text="Hillshade", variable=self.hillshade_var).grid(row=3, column=2, columnspan=2, sticky="w", **pad)

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
        ttk.Button(buttons, text="Cancel", command=self.on_cancel).pack(side="left", padx=8)

        # Advanced toggle moved to bottom
        adv_toggle = ttk.Button(buttons, text="Advanced ▾", command=self.toggle_advanced)
        adv_toggle.pack(side="right")

        self.adv_frame = ttk.LabelFrame(bottom, text="Advanced")
        self.adv_frame.pack(fill="x", pady=(8, 0))
        self.adv_frame.pack_forget()

        self.adv_frame.columnconfigure(0, weight=0)
        self.adv_frame.columnconfigure(1, weight=0)
        self.adv_frame.columnconfigure(2, weight=1)

        ttk.Label(self.adv_frame, text="Workers:").grid(row=0, column=0, sticky="w", **pad)
        ttk.Spinbox(self.adv_frame, from_=1, to=8, textvariable=self.workers_var, width=6).grid(row=0, column=1, sticky="w", **pad)

        ttk.Checkbutton(self.adv_frame, text="Fast scan (coarse-to-fine) (buggy)", variable=self.fast_scan_var).grid(row=0, column=2, sticky="w", **pad)
        ttk.Checkbutton(self.adv_frame, text="Aggressive mode (faster)", variable=self.aggressive_var).grid(row=1, column=2, sticky="w", **pad)

        ttk.Label(self.adv_frame, text="FPS (advanced):").grid(row=1, column=0, sticky="w", **pad)
        ttk.Entry(self.adv_frame, textvariable=self.fps_var, width=10).grid(row=1, column=1, sticky="w", **pad)

        ttk.Checkbutton(self.adv_frame, text="Debug block IDs + unknowns", variable=self.debug_blocks_var).grid(row=2, column=0, columnspan=2, sticky="w", **pad)
        self.debug_btn = ttk.Button(self.adv_frame, text="Debug: render 1 chunk…", command=self.on_debug_one_chunk)
        self.debug_btn.grid(row=2, column=2, sticky="w", **pad)
        # Log output (hidden in Advanced by default)
        self.adv_frame.columnconfigure(0, weight=1)
        self.adv_frame.rowconfigure(3, weight=1)
        logbox = ttk.LabelFrame(self.adv_frame, text="Log")
        logbox.grid(row=3, column=0, columnspan=3, sticky="nsew", padx=0, pady=(8, 0))
        sb = ttk.Scrollbar(logbox, orient="vertical")
        sb.pack(side="right", fill="y")
        self.log_text = tk.Text(logbox, height=10, wrap="word", yscrollcommand=sb.set)
        sb.config(command=self.log_text.yview)
        self.log_text.pack(fill="both", expand=True, padx=6, pady=6)

        # Hide custom W/H unless Custom selected
        self.target_var.trace_add("write", lambda *_: self._sync_custom())
        self._sync_custom()
        self._sync_debug_button_visibility()
        self.debug_blocks_var.trace_add("write", lambda *_: self._sync_debug_button_visibility())

    def _build_single_tab(self, parent):
        pad = {"padx": 8, "pady": 6}

        top = ttk.Frame(parent)
        top.pack(fill="x", **pad)

        ttk.Label(top, text="Backup ZIP:").grid(row=0, column=0, sticky="w", **pad)
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
        ttk.Checkbutton(opts, text="Hillshade", variable=self.single_hillshade_var).grid(row=2, column=1, sticky="w", **pad)

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

    def _sync_custom(self):
        is_custom = self.target_var.get().strip().lower().startswith("custom")
        if is_custom:
            self.custom_wh_frame.grid()
        else:
            self.custom_wh_frame.grid_remove()

    def _sync_single_custom(self):
        is_custom = self.single_target_var.get().strip().lower().startswith("custom")
        if is_custom:
            self.single_custom_wh.grid()
        else:
            self.single_custom_wh.grid_remove()

    def toggle_advanced(self):
        self.advanced_open.set(not self.advanced_open.get())
        if self.advanced_open.get():
            self.adv_frame.pack(fill="x", pady=(8, 0))
        else:
            self.adv_frame.pack_forget()

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
            title="Select folder containing ZIP backups",
            initialdir=(self.folder_var.get().strip() or None),
        )
        if d:
            self.folder_var.set(d)
            try:
                cfg = _load_config()
                cfg["last_input_dir"] = d
                _save_config(cfg)
            except Exception:
                pass

    def pick_output(self):
        d = filedialog.askdirectory(
            title="Select output folder",
            initialdir=(self.out_var.get().strip() or None),
        )
        if d:
            self.out_var.set(d)
            try:
                cfg = _load_config()
                cfg["last_output_dir"] = d
                _save_config(cfg)
            except Exception:
                pass

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
        f = filedialog.askopenfilename(title="Select a world backup ZIP", filetypes=[("ZIP files", "*.zip")])
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

    # ---------- Actions ----------
    def on_cancel(self):
        if self.worker_thread and self.worker_thread.is_alive():
            self.cancel_event.set()
            self._set_status("Cancel requested… stopping soon.", "If a chunk is being processed, it will stop as soon as possible.")
        else:
            self.cancel_event.set()

    def _gather_options(self) -> RenderOptions:
        opt = RenderOptions()
        opt.dimension = self.dimension_var.get()
        opt.y_min = int(self.ymin_var.get())
        opt.y_max = int(self.ymax_var.get())
        opt.skip_water = bool(self.skip_water_var.get())
        opt.hillshade = bool(self.hillshade_var.get())
        # Target preset
        tgt = self.target_var.get()
        if tgt.strip().lower().startswith("custom"):
            opt.target_preset = f"Custom ({int(self.custom_w_var.get())}x{int(self.custom_h_var.get())})"
        else:
            opt.target_preset = tgt

        opt.workers = int(self.workers_var.get())
        opt.fast_scan = bool(self.fast_scan_var.get())
        opt.aggressive_mode = bool(self.aggressive_var.get())
        opt.debug_block_samples = bool(self.debug_blocks_var.get())

        opt.limit_enabled = bool(self.limit_enabled_var.get())
        opt.x_min = int(self.xmin_var.get())
        opt.z_min = int(self.zmin_var.get())
        opt.x_max = int(self.xmax_var.get())
        opt.z_max = int(self.zmax_var.get())
        return opt

    def _validate_timelapse_inputs(self) -> Optional[str]:
        folder = self.folder_var.get().strip()
        if not folder or not os.path.isdir(folder):
            return "Please select a folder containing ZIP backups."
        out = self.out_var.get().strip()
        if not out:
            return "Please select an output folder."
        try:
            os.makedirs(out, exist_ok=True)
        except Exception:
            return "Could not create/access output folder."
        zips = find_zip_backups(folder)
        if not zips:
            return "No ZIP files were found in the selected folder."
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
        zips = find_zip_backups(folder)  # newest first
        opt = self._gather_options()

        seconds_per_frame = float(self.seconds_per_frame_var.get())

        # Clear cancel + log
        self.cancel_event.clear()
        self._set_busy(True)
        self._set_progress(0.0)
        self._set_status("Starting…", "")
        self._log("-" * 60, "timelapse")
        self._log(f"Backups: {len(zips)} (processing newest → oldest)", "timelapse")

        # Start worker
        def runner():
            worker_run(zips, out_dir, opt, seconds_per_frame, self.msgq, self.cancel_event)

        self.worker_thread = threading.Thread(target=runner, daemon=True)
        self.worker_thread.start()

    def on_render_single(self):
        zip_path = self.single_zip_var.get().strip()
        out_png = self.single_out_png_var.get().strip()
        if not zip_path or not os.path.isfile(zip_path):
            messagebox.showerror("Missing input", "Please select a backup ZIP.")
            return
        if not out_png:
            messagebox.showerror("Missing input", "Please choose an output PNG filename.")
            return

        opt = RenderOptions()
        opt.dimension = self.single_dimension_var.get()
        opt.y_min = int(self.single_ymin_var.get())
        opt.y_max = int(self.single_ymax_var.get())
        opt.skip_water = bool(self.single_skip_water_var.get())
        opt.hillshade = bool(self.single_hillshade_var.get())

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

        # Use a worker thread and reuse message queue with "single_*" tags
        self.cancel_event.clear()
        self._log("-" * 60, "single")
        self._log(f"Rendering single map from: {zip_path}", "single")
        self._set_status("Rendering single map…", "")

        def single_runner():
            try:
                tmpdir = tempfile.mkdtemp(prefix="wmtt4mc_single_")
                extract_root, roots = unzip_world_find_roots(zip_path, tmpdir)
                if not roots:
                    raise RuntimeError("Could not find a world folder (no level.dat found in ZIP).")
                # pick best root by score
                roots_scored = sorted([(r, _score_world_root_path(r)) for r in roots], key=lambda t: t[1], reverse=True)
                world_root = roots_scored[0][0]
                bounds = render_world_map(world_root, out_png, opt, log_cb=lambda m: self.msgq.put(("log", m)), progress_cb=lambda p, t, pct: self.msgq.put(("progress", p, t, pct)), cancel_event=self.cancel_event)
                self.msgq.put(("single_done", out_png))
            except Exception:
                self.msgq.put(("single_error", traceback.format_exc()))
            finally:
                try:
                    shutil.rmtree(tmpdir, ignore_errors=True)
                except Exception:
                    pass

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
        self._set_busy(True)
        self._set_progress(0.0)
        self._set_status("Debugging…", "Rendering one chunk PNG + debug IDs.")
        self._log(f"Debug ZIP: {zip_path}", "timelapse")

        def dbg_runner():
            debug_one_chunk_worker(zip_path, out_dir, opt, self.msgq, self.cancel_event)

        self.worker_thread = threading.Thread(target=dbg_runner, daemon=True)
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
                    self._set_status("Error.", "See log for details.")
                    self._log(payload, "timelapse")
                elif kind == "done_debug":
                    self._set_busy(False)
                    self._set_status("Debug done.", "")
                    self._log(payload, "timelapse")
                elif kind == "single_done":
                    self._set_busy(False)
                    self._set_status("Single map saved.", payload)
                    self._log(f"Saved: {payload}", "single")
                elif kind == "single_error":
                    self._set_busy(False)
                    self._set_status("Error.", "Single map render failed.")
                    self._log(payload, "single")
                else:
                    # Unknown message type
                    pass
        except queue.Empty:
            pass
        self.after(100, self._poll_messages)

    def main(self):
        self.mainloop()


def main():
    palette_path = os.path.join(_app_dir(), "palette.json")
    ensure_palette_json(palette_path)
    apply_palette_overrides(palette_path)
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()