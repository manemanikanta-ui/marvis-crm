@echo off
echo.
echo  ╔══════════════════════════════════════════╗
echo  ║        MARVIS CRM — Starting...          ║
echo  ╚══════════════════════════════════════════╝
echo.

cd backend
pip install -r requirements.txt -q

echo  ✅ Starting CRM server at http://localhost:8000
echo  🌐 Opening dashboard...
echo.

start "" "http://localhost:8000/dashboard"
python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload
