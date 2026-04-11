
import threading
from collections import Counter
from typing import Any, Callable, List, Optional, Tuple

def iter_sample_positions(base: int, size: int, min_axis: int, bpp: int) -> list:
    offset = (min_axis - base) % bpp
    start = base + offset
    out = []
    for v in range(start, base + size, bpp):
        out.append(v)
    return out

def worker_fn(
    worker_id: int, coords: List[Tuple[int, int]],
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
    iter_sample_positions_fn,
    classify_block_fn,
):
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
                ch = get_chunk_cached(cx, cz)
            else:
                with chunk_access_lock:
                    ch = get_chunk_cached(cx, cz)
        except Exception:
            local_skipped += 1
            with counters_lock:
                shared_counters["processed_chunks"] = shared_counters.get("processed_chunks", 0) + 1
            continue
        if ch is None:
            local_skipped += 1
            with counters_lock:
                shared_counters["processed_chunks"] = shared_counters.get("processed_chunks", 0) + 1
            continue
        local_rendered += 1
        base_x = cx * 16
        base_z = cz * 16
        chunk_get_block = getattr(ch, "get_block", None)
        block_getter_fn: Optional[Callable[[int, int, int], Any]] = chunk_get_block if callable(chunk_get_block) else None
        if block_getter_fn is None and callable(world_block_getter):
            def _fallback_get_block(lx, y, lz, _bx=base_x, _bz=base_z):
                return world_block_getter(_bx + int(lx), int(y), _bz + int(lz))
            block_getter_fn = _fallback_get_block

        xs = iter_sample_positions_fn(base_x, 16, min_x, bpp)
        zs = iter_sample_positions_fn(base_z, 16, min_z, bpp)
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
                    block_getter=block_getter_fn,
                )
                if found:
                    raw_str = str(raw_block)
                    try:
                        rgb_px, norm_id, is_known, _reason = classify_block_fn(raw_block)
                    except Exception:
                        idx = PALETTE_KEY_TO_IDX.get(raw_str)
                        if idx is not None:
                            rgb_px = PALETTE_COLOR_TABLE[idx]
                            norm_id = raw_str
                            is_known = True
                        else:
                            rgb_px = (128, 128, 128)
                            norm_id = raw_str
                            is_known = False
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
                        base_id = str(norm_id).split('[', 1)[0].split('{', 1)[0]
                        if base_id in PALETTE_KEY_TO_IDX:
                            local_exact_key_miss_counts[base_id] += 1
                        else:
                            local_base_id_miss_counts[base_id] += 1
                        local_unknown_raw_counts[raw_str] += 1
                else:
                    local_air += 1
                    rgb[iz, ix, :] = (0, 0, 0)
                    hmap[iz, ix] = opt.y_min
        if not opt.aggressive_mode:
            import time
            time.sleep(0.0005)
        with counters_lock:
            shared_counters["processed_chunks"] = shared_counters.get("processed_chunks", 0) + 1
        if debug_enabled:
            import time
            now = time.time()
            if (now - render_start) >= DEBUG_FORCE_FIRST_WRITE_AFTER_SEC:
                with counters_lock:
                    maybe_write_debug_snapshot(force=True)
            else:
                with counters_lock:
                    maybe_write_debug_snapshot(force=False)
        if progress_cb is not None:
            import time
            now = time.time()
            with progress_lock:
                last_emit = float(progress_state.get("last_progress_emit", 0.0))
                if now - last_emit >= 0.4:
                    processed_now = int(shared_counters.get("processed_chunks", 0))
                    pct = processed_now / max(1, chunks_total)
                    progress_cb(processed_now, chunks_total, pct)
                    progress_state["last_progress_emit"] = now
    with counters_lock:
        shared_counters["chunks_rendered"] = shared_counters.get("chunks_rendered", 0) + local_rendered
        shared_counters["chunks_skipped"] = shared_counters.get("chunks_skipped", 0) + local_skipped
        shared_counters["colored_cols"] = shared_counters.get("colored_cols", 0) + local_colored
        shared_counters["air_only_cols"] = shared_counters.get("air_only_cols", 0) + local_air
        shared_counters["unknown_cols"] = shared_counters.get("unknown_cols", 0) + local_unknown
        unknown_norm_counts.update(local_unknown_counts)
        unknown_raw_counts.update(local_unknown_raw_counts)
        exact_key_miss_counts.update(local_exact_key_miss_counts)
        base_id_miss_counts.update(local_base_id_miss_counts)
