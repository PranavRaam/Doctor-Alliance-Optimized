# Doctor Alliance PDF Processing Pipeline - Troubleshooting Guide

## 🚨 PyMuPDF DLL Error Resolution

### Problem
```
ImportError: DLL load failed while importing _extra: The specified module could not be found.
```

### Solutions (Try in Order)

#### 1. **Install Visual C++ Redistributables (Most Common Fix)**
- Download: https://aka.ms/vs/17/release/vc_redist.x64.exe
- Install and restart your computer
- Try running the pipeline again

#### 2. **Reinstall PyMuPDF**
```bash
pip uninstall PyMuPDF fitz -y
pip install PyMuPDF==1.23.8
```

#### 3. **Use the Installation Script**
```bash
python install_dependencies.py
```

#### 4. **Alternative: Use Fallback Mode**
The code has been modified to work without PyMuPDF using PDFPlumber and PDFMiner as alternatives.

### Automatic Fallback
If PyMuPDF fails to load, the system will automatically:
- Use PDFPlumber for text extraction
- Use PDFMiner as backup
- Use Tesseract OCR for image-based PDFs
- Log warnings but continue processing

## 🔧 Other Common Issues

### Tesseract OCR Not Found
```bash
# Windows: Download from https://github.com/UB-Mannheim/tesseract/wiki
# Set environment variable:
set TESSERACT_CMD=C:\Program Files\Tesseract-OCR\tesseract.exe
```

### Missing Dependencies
```bash
pip install -r requirements.txt
```

### Permission Issues
- Run as Administrator on Windows
- Check file permissions on PDF directories

## 📋 System Requirements

### Windows
- Python 3.8+
- Visual C++ Redistributables 2015-2022
- Tesseract OCR (optional but recommended)

### Linux/Mac
- Python 3.8+
- Tesseract OCR: `sudo apt-get install tesseract-ocr`

## 🚀 Quick Start

1. **Install dependencies:**
   ```bash
   python install_dependencies.py
   ```

2. **Test the setup:**
   ```bash
   python -c "import fitz; print('PyMuPDF OK')"
   ```

3. **Run the pipeline:**
   ```bash
   python pipeline_main.py
   ```

## 📞 Support

If you continue to have issues:
1. Check the logs for specific error messages
2. Try the fallback mode (PyMuPDF will be skipped)
3. Ensure all system requirements are met

## 🔄 Fallback Mode Details

When PyMuPDF is unavailable, the system uses:
- **PDFPlumber**: Primary text extraction
- **PDFMiner**: Secondary text extraction  
- **Tesseract OCR**: For image-based PDFs
- **Quality Analysis**: Still performed on all extractions

This ensures the pipeline continues to work even with PyMuPDF issues. 