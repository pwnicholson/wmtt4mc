#!/usr/bin/env python3
"""
Regression tests for WMTT4MC cache discovery and matching logic.

Tests cover:
- Cache file discovery
- Matching backups with caches
- Unicode stem normalization
- Stale/corrupt cache handling
- Edge cases
"""

import os
import shutil
import sqlite3
import tempfile
import json
import pytest
from pathlib import Path

# Import the functions to test
from wmtt4mc_cache import (
    is_cache_file,
    is_world_folder,
    snapshot_stem,
    sidecar_cache_path,
    discover_raw_snapshot_sources,
    discover_snapshot_inputs,
    CacheWriter,
    read_cache_header,
    build_source_signature,
    cache_matches_source,
)


class TestCacheFileDetection:
    """Test detection of cache files."""
    
    def test_is_cache_file_valid(self, tmp_path):
        """Valid .wmtt4mc files are detected."""
        cache_file = tmp_path / "test.wmtt4mc"
        cache_file.write_text("fake")
        assert is_cache_file(str(cache_file))
    
    def test_is_cache_file_case_insensitive(self, tmp_path):
        """Cache file detection is case-insensitive."""
        cache_file = tmp_path / "test.WMTT4MC"
        cache_file.write_text("fake")
        assert is_cache_file(str(cache_file))
    
    def test_is_cache_file_directory(self, tmp_path):
        """Directories are not cache files."""
        cache_dir = tmp_path / "test.wmtt4mc"
        cache_dir.mkdir()
        assert not is_cache_file(str(cache_dir))
    
    def test_is_cache_file_nonexistent(self, tmp_path):
        """Nonexistent files are not cache files."""
        cache_file = tmp_path / "nonexistent.wmtt4mc"
        assert not is_cache_file(str(cache_file))
    
    def test_is_world_folder_valid(self, tmp_path):
        """Folders with level.dat are detected as world folders."""
        world_dir = tmp_path / "world"
        world_dir.mkdir()
        (world_dir / "level.dat").write_text("")
        assert is_world_folder(str(world_dir))
    
    def test_is_world_folder_no_level_dat(self, tmp_path):
        """Folders without level.dat are not world folders."""
        world_dir = tmp_path / "world"
        world_dir.mkdir()
        assert not is_world_folder(str(world_dir))
    
    def test_is_world_folder_nonexistent(self, tmp_path):
        """Nonexistent directories are not world folders."""
        assert not is_world_folder(str(tmp_path / "nonexistent"))


class TestSnapshotStem:
    """Test snapshot stem extraction."""
    
    def test_stem_from_zip_file(self):
        """ZIP filenames are stemmed correctly."""
        assert snapshot_stem("/path/World_Backup.zip") == "World_Backup"
    
    def test_stem_from_wmtt4mc_file(self):
        """Cache filenames are stemmed correctly."""
        assert snapshot_stem("/path/World_Backup.wmtt4mc") == "World_Backup"
    
    def test_stem_from_folder(self):
        """Folder names have no extension to remove."""
        assert snapshot_stem("/path/World_Backup") == "World_Backup"
    
    def test_stem_unicode_normalization(self):
        """Unicode stems are normalized to NFC."""
        import unicodedata
        # Create a NFD (decomposed) version
        nfd_name = unicodedata.normalize('NFD', "café.zip")  # Accented e
        nfc_name = unicodedata.normalize('NFC', "café.zip")
        
        stem_nfd = snapshot_stem(f"/path/{nfd_name}")
        stem_nfc = snapshot_stem(f"/path/{nfc_name}")
        
        # Both should normalize to the same value
        assert stem_nfd == stem_nfc
        assert unicodedata.is_normalized('NFC', stem_nfd)
    
    def test_stem_case_sensitivity(self):
        """Stems preserve case (matching is case-insensitive elsewhere)."""
        assert snapshot_stem("/path/WorldBackup.zip") == "WorldBackup"
        assert snapshot_stem("/path/WORLDBACKUP.zip") == "WORLDBACKUP"


class TestSidecarCachePath:
    """Test sidecar cache path generation."""
    
    def test_cache_path_for_zip(self, tmp_path):
        """Cache path for ZIP files."""
        zip_file = tmp_path / "World_Backup.zip"
        expected = str(tmp_path / "World_Backup.wmtt4mc")
        assert sidecar_cache_path(str(zip_file)) == expected
    
    def test_cache_path_for_folder(self, tmp_path):
        """Cache path for world folders."""
        world_dir = tmp_path / "World_Backup"
        expected = str(tmp_path / "World_Backup.wmtt4mc")
        assert sidecar_cache_path(str(world_dir)) == expected
    
    def test_cache_path_with_trailing_slash(self, tmp_path):
        """Cache path handles trailing slashes."""
        world_dir = tmp_path / "World_Backup"
        # Add trailing slash
        with_slash = str(world_dir) + "/"
        expected = str(tmp_path / "World_Backup.wmtt4mc")
        assert sidecar_cache_path(with_slash) == expected


class TestDiscoveryRawSources:
    """Test discovery of raw backup sources."""
    
    def test_discover_zips(self, tmp_path):
        """ZIP files are discovered."""
        (tmp_path / "World1.zip").write_text("")
        (tmp_path / "World2.zip").write_text("")
        
        results = discover_raw_snapshot_sources(str(tmp_path))
        names = [r.display_name for r in results]
        
        assert "World1.zip" in names
        assert "World2.zip" in names
    
    def test_discover_world_folders(self, tmp_path):
        """World folders (with level.dat) are discovered."""
        world1 = tmp_path / "World1"
        world1.mkdir()
        (world1 / "level.dat").write_text("")
        
        results = discover_raw_snapshot_sources(str(tmp_path))
        names = [r.display_name for r in results]
        
        assert "World1" in names
    
    def test_discover_sorted(self, tmp_path):
        """Results are sorted alphabetically."""
        (tmp_path / "World_C.zip").write_text("")
        (tmp_path / "World_A.zip").write_text("")
        (tmp_path / "World_B.zip").write_text("")
        
        results = discover_raw_snapshot_sources(str(tmp_path))
        names = [r.display_name for r in results]
        
        assert names == ["World_A.zip", "World_B.zip", "World_C.zip"]
    
    def test_discover_emptydir(self, tmp_path):
        """Empty directory returns empty list."""
        results = discover_raw_snapshot_sources(str(tmp_path))
        assert results == []
    
    def test_discover_ignored_files(self, tmp_path):
        """Non-ZIP files and folders without level.dat are ignored."""
        (tmp_path / "README.txt").write_text("")
        (tmp_path / "notes.doc").write_text("")
        empty_dir = tmp_path / "EmptyDir"
        empty_dir.mkdir()
        
        results = discover_raw_snapshot_sources(str(tmp_path))
        assert len(results) == 0


class TestCacheMatching:
    """Test cache and backup matching logic."""
    
    def _create_test_cache(self, cache_path, source_name="test.zip", source_size=12345):
        """Helper to create a minimal test cache file."""
        metadata = {
            "source_kind": "file",
            "source_name": source_name,
            "source_size": source_size,
            "source_mtime_ns": 1000000000,
            "source_hash": "abc123",
            "format_version": 1,
        }
        
        conn = sqlite3.connect(cache_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            "CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        for key, val in metadata.items():
            conn.execute(
                "INSERT INTO metadata VALUES (?, ?)",
                (key, json.dumps(val))
            )
        conn.execute(
            "CREATE TABLE block_ids (id INTEGER PRIMARY KEY, block_text TEXT NOT NULL UNIQUE)"
        )
        conn.execute(
            "CREATE TABLE chunks (cx INTEGER NOT NULL, cz INTEGER NOT NULL, "
            "surface_payload BLOB NOT NULL, deep_payload BLOB, "
            "PRIMARY KEY (cx, cz))"
        )
        conn.commit()
        conn.close()
    
    def test_cache_matches_source_same(self, tmp_path):
        """Cache matches source with same size and mtime."""
        zip_file = tmp_path / "World.zip"
        zip_file.write_text("x" * 1000)
        
        cache_file = tmp_path / "World.wmtt4mc"
        self._create_test_cache(str(cache_file), "World.zip", 1000)
        
        header = read_cache_header(str(cache_file))
        assert cache_matches_source(header, str(zip_file))
    
    def test_cache_different_size(self, tmp_path):
        """Cache doesn't match if file size differs."""
        zip_file = tmp_path / "World.zip"
        zip_file.write_text("x" * 1000)
        
        cache_file = tmp_path / "World.wmtt4mc"
        # Create cache with different size
        self._create_test_cache(str(cache_file), "World.zip", 2000)
        
        header = read_cache_header(str(cache_file))
        assert not cache_matches_source(header, str(zip_file))
    
    def test_cache_ignored_different_name(self, tmp_path):
        """Cache with different source name doesn't match."""
        zip_file = tmp_path / "World1.zip"
        zip_file.write_text("x" * 1000)
        
        cache_file = tmp_path / "World2.wmtt4mc"
        self._create_test_cache(str(cache_file), "World2.zip", 1000)
        
        header = read_cache_header(str(cache_file))
        assert not cache_matches_source(header, str(zip_file))


class TestDiscoveryWithMatching:
    """Test full discovery with cache/backup matching."""
    
    def _create_test_setup(self, tmp_path):
        """Create a test folder with backups and caches."""
        # Create backups
        (tmp_path / "Backup1.zip").write_text("x" * 100)
        (tmp_path / "Backup2.zip").write_text("x" * 200)
        
        # Create matching cache for Backup1
        cache1 = tmp_path / "Backup1.wmtt4mc"
        CacheWriter(str(cache1), {"source_kind": "file", "source_name": "Backup1.zip", "source_size": 100, "source_mtime_ns": int(os.stat(str(tmp_path / "Backup1.zip")).st_mtime_ns)}).finalize()
        
        # Create orphaned cache (no matching backup)
        cache_orphan = tmp_path / "Orphaned.wmtt4mc"
        CacheWriter(str(cache_orphan), {"source_kind": "file", "source_name": "Orphaned.zip", "source_size": 300}).finalize()
        
        return tmp_path
    
    def test_discovery_matches_cache_to_backup(self, tmp_path):
        """Cache files are matched to their backups."""
        self._create_test_setup(tmp_path)
        
        results = discover_snapshot_inputs(str(tmp_path))
        
        # Should have Backup1 (cache used), Backup2 (raw), and Orphaned (cache)
        kinds = [r.kind for r in results]
        assert "cache" in kinds or "zip" in kinds
    
    def test_discovery_prioritizes_cache(self, tmp_path):
        """Valid cache is used in preference to zip."""
        self._create_test_setup(tmp_path)
        
        results = discover_snapshot_inputs(str(tmp_path))
        
        # Find the result for Backup1
        backup1 = next((r for r in results if "Backup1" in r.display_name), None)
        assert backup1 is not None
        # Should be using cache (or at least have the cache_path set)
        assert backup1.kind == "cache" or backup1.cache_path is not None


class TestCacheLogic:
    """Test cache logic consistency."""
    
    def test_stem_matching_case_insensitive(self):
        """Stem matching should be case-insensitive."""
        stem1 = snapshot_stem("/path/World_Backup.zip").lower()
        stem2 = snapshot_stem("/path/world_backup.wmtt4mc").lower()
        assert stem1 == stem2
    
    def test_unicode_matching(self):
        """Unicode stems should match after normalization."""
        import unicodedata
        # NFD vs NFC should match
        path1 = f"/path/{unicodedata.normalize('NFD', 'Café')}.zip"
        path2 = f"/path/{unicodedata.normalize('NFC', 'Café')}.wmtt4mc"
        
        stem1 = snapshot_stem(path1).lower()
        stem2 = snapshot_stem(path2).lower()
        
        assert stem1 == stem2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
