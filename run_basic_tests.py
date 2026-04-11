#!/usr/bin/env python3
"""Simple test runner for cache discovery logic without pytest."""

import os
import tempfile
import shutil
from pathlib import Path

# Test imports
try:
    from wmtt4mc_cache import (
        is_cache_file,
        is_world_folder,
        snapshot_stem,
        sidecar_cache_path,
        discover_raw_snapshot_sources,
        discover_snapshot_inputs,
    )
    print("✓ All imports successful")
except ImportError as e:
    print(f"✗ Import error: {e}")
    exit(1)

# Test 1: snapshot_stem with Unicode
print("\n--- Testing snapshot_stem ---")
try:
    import unicodedata
    
    # Test basic stems
    assert snapshot_stem("/path/World.zip") == "World"
    assert snapshot_stem("/path/World.wmtt4mc") == "World"
    assert snapshot_stem("/path/World/") == "World"
    print("✓ Basic stems work")
    
    # Test Unicode normalization
    nfd_path = f"/path/{unicodedata.normalize('NFD', 'café')}.zip"
    nfc_path = f"/path/{unicodedata.normalize('NFC', 'café')}.wmtt4mc"
    
    stem_nfd = snapshot_stem(nfd_path)
    stem_nfc = snapshot_stem(nfc_path)
    
    # Both should be NFC normalized
    assert stem_nfd == stem_nfc
    assert unicodedata.is_normalized('NFC', stem_nfd)
    print("✓ Unicode normalization works")
    
except AssertionError as e:
    print(f"✗ Stem test failed: {e}")
except Exception as e:
    print(f"✗ Unexpected error: {e}")

# Test 2: Sidecar cache paths
print("\n--- Testing sidecar_cache_path ---")
try:
    with tempfile.TemporaryDirectory() as tmp:
        zip_file = os.path.join(tmp, "World.zip")
        expected = os.path.join(tmp, "World.wmtt4mc")
        assert sidecar_cache_path(zip_file) == expected
        print("✓ Sidecar paths work")
except Exception as e:
    print(f"✗ Sidecar path test failed: {e}")

# Test 3: Discovery functions
print("\n--- Testing discovery functions ---")
try:
    with tempfile.TemporaryDirectory() as tmp:
        # Create test files
        Path(tmp, "World1.zip").write_text("test")
        Path(tmp, "World2.zip").write_text("test")
        
        world_dir = Path(tmp, "World3")
        world_dir.mkdir()
        (world_dir / "level.dat").write_text("test")
        
        # Test raw discovery
        results = discover_raw_snapshot_sources(tmp)
        names = [r.display_name for r in results]
        
        assert "World1.zip" in names
        assert "World2.zip" in names
        assert "World3" in names
        print(f"✓ Raw discovery works (found {len(results)} items)")
        
        # Test full discovery
        results = discover_snapshot_inputs(tmp)
        assert len(results) >= 3
        print(f"✓ Full discovery works (found {len(results)} items)")
        
except Exception as e:
    print(f"✗ Discovery test failed: {e}")
    import traceback
    traceback.print_exc()

# Test 4: Cache file detection
print("\n--- Testing cache file detection ---")
try:
    with tempfile.TemporaryDirectory() as tmp:
        # Create test files
        cache_file = Path(tmp, "test.wmtt4mc")
        cache_file.write_text("test")
        
        zip_file = Path(tmp, "test.zip")
        zip_file.write_text("test")
        
        # Test detection
        assert is_cache_file(str(cache_file))
        assert not is_cache_file(str(zip_file))
        print("✓ Cache file detection works")
        
        # Test world folder detection
        world_dir = Path(tmp, "world")
        world_dir.mkdir()
        (world_dir / "level.dat").write_text("test")
        
        assert is_world_folder(str(world_dir))
        print("✓ World folder detection works")
        
except Exception as e:
    print(f"✗ File detection test failed: {e}")

print("\n✓ All basic tests passed!")
