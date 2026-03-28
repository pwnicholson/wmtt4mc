#!/usr/bin/env python3
import os
import sys
import json
import platform
from pathlib import Path

def check_python_environment():
    print('🔍 Checking Python Environment...')
    version = sys.version_info
    python_ok = False
    issues = []
    recommendations = []
    
    if version.major < 3 or (version.major == 3 and version.minor < 11):
        issues.append('❌ Python 3.11+ required')
        recommendations.append('Upgrade to Python 3.11+ for best compatibility')
    else:
        python_ok = True
    
    print('   Python ' + str(version.major) + '.' + str(version.minor) + '.' + str(version.micro))
    print('   Platform: ' + platform.system() + ' ' + platform.machine())
    
    return {
        'python_version': str(version.major) + '.' + str(version.minor) + '.' + str(version.micro),
        'python_path': sys.executable,
        'platform': platform.system(),
        'architecture': platform.machine(),
        'python_ok': python_ok,
        'issues': issues,
        'recommendations': recommendations
    }

def main():
    print('🚀 Starting WMTT4MC Development Environment Check...')
    env = check_python_environment()
    print('Python version:', env['python_version'])
    print('Platform:', env['platform'])
    if env['python_ok']:
        print('✅ Python version OK')
    else:
        print('❌ Python version issue:', env['issues'])
    
    print('✅ Development check completed!')

if __name__ == '__main__':
    main()
