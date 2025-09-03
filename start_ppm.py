#!/usr/bin/env python3
"""
Master startup script for PPM (Psychographic Prediction Machine)
Starts both backend API and frontend React app
"""
import os
import sys
import subprocess
import threading
import time
from pathlib import Path


def start_backend():
    """Start the backend API server"""
    print("🔧 Starting Backend API...")
    try:
        subprocess.run([sys.executable, "start_backend.py"])
    except Exception as e:
        print(f"❌ Backend failed to start: {e}")


def start_frontend():
    """Start the frontend React app"""
    print("🎨 Starting Frontend React App...")
    time.sleep(5)  # Give backend time to start
    try:
        subprocess.run([sys.executable, "start_frontend.py"])
    except Exception as e:
        print(f"❌ Frontend failed to start: {e}")


def main():
    print("🚀 Starting PPM - Psychographic Prediction Machine")
    print("=" * 60)
    print("🎯 This will start both the backend API and frontend React app")
    print("🌐 Backend API: http://localhost:8000")
    print("🗺️  Frontend App: http://localhost:3000")
    print("📊 API Docs: http://localhost:8000/docs")
    print("=" * 60)

    # Change to project root
    project_root = Path(__file__).parent
    os.chdir(project_root)

    try:
        # Start backend in a separate thread
        backend_thread = threading.Thread(target=start_backend, daemon=True)
        backend_thread.start()

        # Wait a moment for backend to initialize
        print("⏳ Waiting for backend to initialize...")
        time.sleep(8)

        # Start frontend in main thread
        start_frontend()

    except KeyboardInterrupt:
        print("\n🛑 Shutting down PPM application...")
        print("✅ Thank you for using PPM!")


if __name__ == "__main__":
    main()
