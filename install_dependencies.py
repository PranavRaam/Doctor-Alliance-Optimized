#!/usr/bin/env python3
"""
Dependency installation script for Doctor Alliance PDF Processing Pipeline
This script helps resolve PyMuPDF DLL issues on Windows
"""

import subprocess
import sys
import os

def run_command(command, description):
    """Run a command and handle errors"""
    print(f"\n🔄 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed: {e}")
        print(f"Error output: {e.stderr}")
        return False

def main():
    print("🚀 Doctor Alliance PDF Processing Pipeline - Dependency Installer")
    print("=" * 70)
    
    # Check if we're on Windows
    if os.name == 'nt':
        print("\n📋 Windows detected - PyMuPDF DLL issues are common on Windows")
        print("💡 If you encounter DLL errors, try these steps:")
        print("   1. Install Visual C++ Redistributables: https://aka.ms/vs/17/release/vc_redist.x64.exe")
        print("   2. Restart your computer")
        print("   3. Run this script again")
    
    # Upgrade pip
    run_command(f"{sys.executable} -m pip install --upgrade pip", "Upgrading pip")
    
    # Uninstall existing PyMuPDF installations
    run_command(f"{sys.executable} -m pip uninstall PyMuPDF fitz -y", "Removing existing PyMuPDF installations")
    
    # Install core dependencies
    dependencies = [
        "PyMuPDF==1.23.8",
        "pdfplumber==0.10.3", 
        "pdfminer.six==20231228",
        "pytesseract==0.3.10",
        "Pillow==10.1.0",
        "numpy==1.24.3",
        "pandas==2.0.3",
        "selenium==4.15.2",
        "requests==2.31.0",
        "python-dotenv==1.0.0",
        "openpyxl==3.1.2",
        "xlsxwriter==3.1.9"
    ]
    
    for dep in dependencies:
        run_command(f"{sys.executable} -m pip install {dep}", f"Installing {dep}")
    
    # Try to install EasyOCR (optional)
    print("\n🔄 Installing EasyOCR (optional OCR engine)...")
    try:
        subprocess.run(f"{sys.executable} -m pip install easyocr==1.7.0", shell=True, check=True)
        print("✅ EasyOCR installed successfully")
    except:
        print("⚠️  EasyOCR installation failed - this is optional and won't affect core functionality")
    
    # Test PyMuPDF import
    print("\n🧪 Testing PyMuPDF import...")
    try:
        import fitz
        print("✅ PyMuPDF imported successfully!")
        print(f"   Version: {fitz.version}")
    except ImportError as e:
        print(f"❌ PyMuPDF import failed: {e}")
        print("\n🔧 Troubleshooting steps:")
        print("   1. Install Visual C++ Redistributables")
        print("   2. Restart your computer")
        print("   3. Try: pip install --force-reinstall PyMuPDF==1.23.8")
        print("   4. If still failing, the code will use PDFPlumber as fallback")
    
    print("\n🎉 Installation completed!")
    print("\n📝 Next steps:")
    print("   1. Install Tesseract OCR: https://github.com/UB-Mannheim/tesseract/wiki")
    print("   2. Set TESSERACT_CMD environment variable if needed")
    print("   3. Run your pipeline: python pipeline_main.py")

if __name__ == "__main__":
    main() 