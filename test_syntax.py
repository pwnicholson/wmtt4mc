"""
Basic self-test: checks for syntax errors and import errors in wmtt4mc.py.
Run with: python test_syntax.py
"""
import sys
import py_compile

print("Checking Python syntax for wmtt4mc.py...")
try:
    py_compile.compile("wmtt4mc.py", doraise=True)
    print("Syntax check: OK")
except py_compile.PyCompileError as e:
    print("Syntax error detected:\n", e)
    sys.exit(1)

print("Testing import of wmtt4mc.py (this will not run the app)...")
try:
    import wmtt4mc
    print("Import test: OK")
except Exception as e:
    print("Import error detected:\n", e)
    sys.exit(2)

print("All basic checks passed.")
