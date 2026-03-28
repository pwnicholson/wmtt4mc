#!/usr/bin/env python3
"""
Simple WMTT4MC Development Check
"""

import os
import sys
import json
import platform
from pathlib import Path

def check_python_environment():
    """Check Python version and environment"""
    print("🔍 Checking Python Environment...")
    
    version = sys.version_info
    python_ok = False
    issues = []
    recommendations = []
    
    if version.major < 3 or (version.major == 3 and version.minor < 11):
        issues.append("❌ Python 3.11+ required")
        recommendations.append("Upgrade to Python 3.11+ for best compatibility")
    else:
        python_ok = True
    
    print(f"   Python {version.major}.{version.minor}.{version.micro}")
    print(f"   Platform: {platform.system()} {platform.machine()}")
    
    return {
        "python_version": f"{version.major}.{version.minor}.{version.micro}",
        "python_path": sys.executable,
        "platform": platform.system(),
        "architecture": platform.machine(),
        "python_ok": python_ok,
        "issues": issues,
        "recommendations": recommendations
    }

def check_required_files():
    """Check if all required files exist"""
    print("\n🔍 Checking Required Files...")
    
    required_files = {
        "wmtt4mc.py": "Main application",
        "palette.json": "Block color palette", 
        "requirements.txt": "Python dependencies"
    }
    
    files_status = {}
    issues = []
    recommendations = []
    
    for filename, description in required_files.items():
        filepath = Path(filename)
        exists = filepath.exists()
        size = filepath.stat().st_size if exists else 0
        
        files_status[filename] = {
            "exists": exists,
            "size": size,
            "description": description
        }
        
        if exists:
            print(f"   ✓ {filename} ({description}) - {size} bytes")
        else:
            issues.append(f"❌ Missing file: {filename}")
            print(f"   ❌ {filename} - MISSING")
    
    return {
        "files": files_status,
        "issues": issues,
        "recommendations": recommendations
    }

def check_dependencies():
    """Check Python dependencies"""
    print("\n🔍 Checking Dependencies...")
    
    # Read requirements.txt
    requirements = {}
    req_file = Path("requirements.txt")
    if req_file.exists():
        with open(req_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    if '>=' in line:
                        pkg, version = line.split('>=')
                        requirements[pkg.strip()] = version.strip()
                    else:
                        requirements[line.strip()] = None
    
    print("Required dependencies from requirements.txt:")
    for pkg, version in requirements.items():
        version_str = f" (>= {version})" if version else ""
        print(f"   - {pkg}{version_str}")
    
    # Test imports
    import_tests = [
        ("numpy", "numpy"),
        ("PIL", "PIL (Pillow)"),
        ("tkinter", "tkinter"),
        ("amulet", "amulet-core"),
        ("threading", "threading (built-in)"),
        ("queue", "queue (built-in)"),
        ("json", "json (built-in)"),
        ("os", "os (built-in)"),
        ("tempfile", "tempfile (built-in)"),
    ]
    
    deps_status = {"required": requirements}
    issues = []
    recommendations = []
    
    for module_name, display_name in import_tests:
        try:
            __import__(module_name)
            deps_status[module_name] = {"status": "ok", "display": display_name}
            print(f"   ✓ {display_name}")
        except ImportError as e:
            deps_status[module_name] = {"status": "missing", "display": display_name, "error": str(e)}
            issues.append(f"❌ Missing dependency: {display_name}")
            print(f"   ❌ {display_name} - {e}")
    
    if issues:
        missing_list = " ".join([info["display"] for module, info in deps_status.items() if info.get("status") == "missing"])
        recommendations.append(f"Install missing dependencies using: pip install {missing_list}")
    
    return {
        "dependencies": deps_status,
        "issues": issues,
        "recommendations": recommendations
    }

def analyze_palette():
    """Analyze the palette.json file"""
    print("\n🔍 Analyzing Palette...")
    
    palette_file = Path("palette.json")
    if not palette_file.exists():
        return {
            "error": "Palette file not found",
            "issues": ["❌ Missing palette.json file"],
            "recommendations": ["Create palette.json file with block color mappings"]
        }
    
    try:
        with open(palette_file, 'r', encoding='utf-8') as f:
            palette_data = json.load(f)
        
        file_size = palette_file.stat().st_size
        rgb_overrides = palette_data.get("rgb_overrides", {})
        total_entries = len(rgb_overrides)
        
        # Analyze palette structure
        wood_blocks = [k for k in rgb_overrides.keys() if any(wood in k.lower() for wood in ['oak', 'birch', 'spruce', 'jungle', 'acacia', 'dark_oak'])]
        stone_blocks = [k for k in rgb_overrides.keys() if 'stone' in k.lower()]
        water_blocks = [k for k in rgb_overrides.keys() if 'water' in k.lower()]
        
        analysis = {
            "file_size": file_size,
            "total_entries": total_entries,
            "wood_variants": len(wood_blocks),
            "stone_variants": len(stone_blocks),
            "water_variants": len(water_blocks),
            "sample_woods": wood_blocks[:5],
            "sample_stones": stone_blocks[:3],
            "sample_waters": water_blocks[:3]
        }
        
        print(f"   ✓ Palette loaded - {total_entries} color entries")
        print(f"   ✓ File size: {file_size} bytes")
        print(f"   ✓ Wood variants: {len(wood_blocks)}")
        print(f"   ✓ Stone variants: {len(stone_blocks)}")
        print(f"   ✓ Water variants: {len(water_blocks)}")
        
        issues = []
        recommendations = []
        
        if total_entries < 100:
            issues.append("⚠️  Palette may be incomplete (<100 entries)")
            recommendations.append("Consider expanding palette coverage for better color variety")
        
        return {
            "analysis": analysis,
            "issues": issues,
            "recommendations": recommendations
        }
        
    except Exception as e:
        return {
            "error": str(e),
            "issues": [f"❌ Palette analysis failed: {e}"],
            "recommendations": ["Check palette.json format and encoding"]
        }

def check_importability():
    """Test if the main module can be imported"""
    print("\n🔍 Testing Module Importability...")
    
    try:
        # Add current directory to Python path
        current_dir = Path.cwd()
        sys.path.insert(0, str(current_dir))
        
        # Try to import the main module
        import wmtt4mc
        
        print(f"   ✓ Module imported successfully")
        print(f"   ✓ Version: {wmtt4mc.APP_VERSION}")
        print(f"   ✓ Build: {wmtt4mc.APP_BUILD}")
        
        # Test key components
        try:
            # Test palette loading
            palette_path = Path("palette.json")
            if palette_path.exists():
                wmtt4mc.apply_palette_overrides(str(palette_path))
                print("   ✓ Palette loading works")
            
            # Test basic block classification
            rgb, key, known, reason = wmtt4mc.classify_block("minecraft:grass_block")
            print(f"   ✓ Block classification works - grass_block -> RGB:{rgb}, known:{known}")
            
            return {
                "status": "success",
                "version": wmtt4mc.APP_VERSION,
                "build": wmtt4mc.APP_BUILD,
                "issues": [],
                "recommendations": []
            }
            
        except Exception as e:
            print(f"   ⚠️  Component test failed: {e}")
            return {
                "status": "partial_success",
                "version": wmtt4mc.APP_VERSION,
                "build": wmtt4mc.APP_BUILD,
                "issues": [f"⚠️  Component test failed: {e}"],
                "recommendations": ["Check component dependencies and functionality"]
            }
            
    except Exception as e:
        print(f"   ❌ Module import failed: {e}")
        return {
            "status": "failed",
            "error": str(e),
            "issues": [f"❌ Module import failed: {e}"],
            "recommendations": ["Check Python path and file permissions"]
        }

def generate_report(results):
    """Generate a comprehensive report"""
    print("\n" + "="*60)
    print("📋 WMTT4MC DEVELOPMENT ENVIRONMENT REPORT")
    print("="*60)
    
    # Environment summary
    print("\n🖥️  ENVIRONMENT:")
    env = results.get("environment", {})
    print(f"   Python: {env.get('python_version', 'Unknown')}")
    print(f"   Platform: {env.get('platform', 'Unknown')} {env.get('architecture', '')}")
    print(f"   Python Path: {env.get('python_path', 'Unknown')}")
    
    # Files summary
    print("\n📁 FILES:")
    files = results.get("files", {}).get("files", {})
    for filename, info in files.items():
        status = "✅" if info.get("exists") else "❌"
        print(f"   {status} {filename} - {info.get('description', 'Unknown')} ({info.get('size', 0)} bytes)")
    
    # Dependencies summary
    print("\n📦 DEPENDENCIES:")
    deps = results.get("dependencies", {}).get("dependencies", {})
    for module, info in deps.items():
        if module != "required":
            status = "✅" if info.get("status") == "ok" else "❌"
            display_name = info.get("display", module)
            print(f"   {status} {display_name}")
    
    # Palette summary
    print("\n🎨 PALETTE:")
    palette = results.get("palette", {})
    if "analysis" in palette:
        analysis = palette["analysis"]
        print(f"   Entries: {analysis.get('total_entries', 0)}")
        print(f"   Wood variants: {analysis.get('wood_variants', 0)}")
        print(f"   Stone variants: {analysis.get('stone_variants', 0)}")
        print(f"   Water variants: {analysis.get('water_variants', 0)}")
    
    # Module import status
    import_test = results.get("import_test", {})
    if import_test:
        status = import_test.get("status", "unknown")
        print(f"\n📦 MODULE IMPORT: {status.upper()}")
        if status == "success":
            print(f"   Version: {import_test.get('version', 'Unknown')}")
            print(f"   Build: {import_test.get('build', 'Unknown')}")
    
    # All issues and recommendations
    all_issues = []
    all_recommendations = []
    
    for section in ["environment", "files", "dependencies", "palette", "import_test"]:
        section_data = results.get(section, {})
        all_issues.extend(section_data.get("issues", []))
        all_recommendations.extend(section_data.get("recommendations", []))
    
    print("\n⚠️  ISSUES FOUND:")
    for issue in all_issues:
        print(f"   {issue}")
    
    print("\n💡 RECOMMENDATIONS:")
    for rec in all_recommendations:
        print(f"   {rec}")
    
    # Overall assessment
    print("\n" + "="*60)
    print("🎯 OVERALL ASSESSMENT:")
    
    critical_issues = [i for i in all_issues if i.startswith("❌")]
    warnings = [i for i in all_issues if i.startswith("⚠️")]
    
    if not critical_issues and not warnings:
        print("✅ Environment looks good! Ready for development.")
    elif critical_issues:
        print("❌ Critical issues found. Address these first:")
        for issue in critical_issues:
            print(f"   {issue}")
    else:
        print("⚠️  Some warnings found. Can proceed but consider addressing:")
        for issue in warnings:
            print(f"   {issue}")
    
    print("="*60)
    
    return len(critical_issues) == 0

def main():
    """Run the complete development check"""
    print("🚀 Starting WMTT4MC Development Environment Check...")
    
    results = {}
    
    # Run all checks
    results["environment"] = check_python_environment()
    results["files"] = check_required_files()
    results["dependencies"] = check_dependencies()
    results["palette"] = analyze_palette()
    results["import_test"] = check_importability()
    
    # Generate report
    success = generate_report(results)
    
    # Save detailed report
    report_file = Path("development_report.json")
    try:
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"\n📄 Detailed report saved to: {report_file}")
    except Exception as e:
        print(f"\n⚠️  Could not save report: {e}")
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
