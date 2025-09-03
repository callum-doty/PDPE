#!/usr/bin/env python3
"""
Startup script for PPM Backend API
"""
import os
import sys
import subprocess
from pathlib import Path


def main():
    print("🚀 Starting PPM Backend API...")

    # Change to the project root directory
    project_root = Path(__file__).parent
    os.chdir(project_root)

    # Check if virtual environment exists
    venv_path = project_root / "venv"
    if not venv_path.exists():
        print("⚠️  Virtual environment not found. Creating one...")
        subprocess.run([sys.executable, "-m", "venv", "venv"])
        print("✅ Virtual environment created")

    # Determine the correct python executable
    if os.name == "nt":  # Windows
        python_exe = venv_path / "Scripts" / "python.exe"
        pip_exe = venv_path / "Scripts" / "pip.exe"
    else:  # Unix/Linux/macOS
        python_exe = venv_path / "bin" / "python"
        pip_exe = venv_path / "bin" / "pip"

    # Install dependencies if needed
    requirements_file = project_root / "requirements.txt"
    if requirements_file.exists():
        print("📦 Installing Python dependencies...")
        subprocess.run([str(pip_exe), "install", "-r", "requirements.txt"])
        print("✅ Dependencies installed")

    # Add additional FastAPI dependencies
    print("📦 Installing FastAPI dependencies...")
    subprocess.run(
        [str(pip_exe), "install", "fastapi", "uvicorn[standard]", "python-multipart"]
    )

    # Set environment variables
    os.environ["PYTHONPATH"] = str(project_root / "src")

    print("🌐 Starting FastAPI server on http://localhost:8000")
    print("📊 API documentation available at http://localhost:8000/docs")
    print("🔄 Press Ctrl+C to stop the server")
    print("-" * 50)

    try:
        # Start the FastAPI server
        subprocess.run(
            [
                str(python_exe),
                "-m",
                "uvicorn",
                "src.backend.models.serve:app",
                "--host",
                "0.0.0.0",
                "--port",
                "8000",
                "--reload",
            ]
        )
    except KeyboardInterrupt:
        print("\n🛑 Backend server stopped")


if __name__ == "__main__":
    main()
