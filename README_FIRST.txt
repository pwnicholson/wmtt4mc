WMTT4MC (World Map Timeline Tool for Minecraft) — Known-good restore pack

This pack is based on v1.6.0.

Contents:
- wmtt4mc.py
- requirements.txt
- app_icon.ico
- app_icon_1024.png

Windows build steps (PowerShell):

1) Create a fresh folder and extract this zip into it, e.g. C:\WMTT4MC

2) Create & activate venv (Python 3.11.x recommended):
   py -3.11 -m venv .venv
   .\.venv\Scripts\Activate.ps1

3) Install deps:
   python -m pip install --upgrade pip
   pip install -r requirements.txt
   pip install pyinstaller

4) Run from source:
   python wmtt4mc.py

5) Clean old build artifacts if any:
   Remove-Item -Recurse -Force .\build, .\dist -ErrorAction SilentlyContinue
   Remove-Item -Force .\*.spec -ErrorAction SilentlyContinue

6) Build:
   python -m PyInstaller --clean --noconsole --onedir --name WMTT4MC `
     --icon app_icon.ico `
     --add-data "app_icon.ico;." `
     --add-data "app_icon_1024.png;." `
     wmtt4mc.py

7) Run:
   .\dist\WMTT4MC\WMTT4MC.exe
