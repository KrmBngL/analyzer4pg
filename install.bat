@echo off
setlocal EnableDelayedExpansion

REM ============================================================
REM analyzer4pg - Windows Kurulum Scripti
REM Gereksinim: Python 3.8+ (python.org veya Microsoft Store)
REM ============================================================

echo.
echo ==========================================
echo   analyzer4pg - Windows Kurulum
echo ==========================================
echo.

REM ---- Python kontrolü ----
set PYTHON=
for %%P in (python3.12 python3.11 python3.10 python3.9 python3.8 python3 python py) do (
    where %%P >nul 2>&1
    if !errorlevel! == 0 (
        %%P -c "import sys; sys.exit(0 if sys.version_info >= (3,8) else 1)" >nul 2>&1
        if !errorlevel! == 0 (
            set PYTHON=%%P
            goto :found_python
        )
    )
)

echo [HATA] Python 3.8+ bulunamadi.
echo.
echo Python indirmek icin: https://www.python.org/downloads/
echo Kurulum sirasinda "Add Python to PATH" secenegini isaretleyin.
echo.
pause
exit /b 1

:found_python
for /f "tokens=*" %%V in ('!PYTHON! --version 2^>^&1') do set PY_VER=%%V
echo [BILGI] Python bulundu: !PY_VER! (!PYTHON!)
echo.

REM ---- pip güncellemesi ----
echo [BILGI] pip guncelleniyor...
!PYTHON! -m pip install --upgrade pip --quiet
if errorlevel 1 (
    echo [UYARI] pip guncellenemedi, devam ediliyor...
)

REM ---- Kurulum yöntemi ----
echo Kurulum yontemi:
echo   1) Kullanici dizinine kur (onerilen, yonetici hakki gerekmez)
echo   2) Sanal ortama (venv) kur
echo   3) Sistem geneline kur (yonetici hakki gerekebilir)
echo.
set /p CHOICE="Secim [1]: "
if "!CHOICE!"=="" set CHOICE=1

REM Script dizinini al
set SCRIPT_DIR=%~dp0
if "%SCRIPT_DIR:~-1%"=="\" set SCRIPT_DIR=%SCRIPT_DIR:~0,-1%

if "!CHOICE!"=="1" (
    echo.
    echo [BILGI] Kullanici dizinine kuruluyor...
    !PYTHON! -m pip install --user -e "!SCRIPT_DIR!"
    if errorlevel 1 goto :install_error
    set INSTALL_TYPE=user
) else if "!CHOICE!"=="2" (
    set VENV_DIR=!SCRIPT_DIR!\.venv
    echo.
    echo [BILGI] Sanal ortam olusturuluyor: !VENV_DIR!
    !PYTHON! -m venv "!VENV_DIR!"
    if errorlevel 1 (
        echo [HATA] Sanal ortam olusturulamadi.
        pause
        exit /b 1
    )
    "!VENV_DIR!\Scripts\pip.exe" install --upgrade pip --quiet
    "!VENV_DIR!\Scripts\pip.exe" install -e "!SCRIPT_DIR!"
    if errorlevel 1 goto :install_error
    echo.
    echo [BILGI] Sanal ortami aktive etmek icin:
    echo   !VENV_DIR!\Scripts\activate.bat
    set INSTALL_TYPE=venv
) else if "!CHOICE!"=="3" (
    echo.
    echo [BILGI] Sistem geneline kuruluyor...
    !PYTHON! -m pip install -e "!SCRIPT_DIR!"
    if errorlevel 1 goto :install_error
    set INSTALL_TYPE=system
) else (
    echo [HATA] Gecersiz secim: !CHOICE!
    pause
    exit /b 1
)

REM ---- Kurulum kontrolü ----
echo.
analyzer4pg --version >nul 2>&1
if errorlevel 1 (
    echo [UYARI] analyzer4pg komutu PATH'te bulunamadi.
    echo.
    echo Cozum secenekleri:
    echo   1. Yeni bir cmd/PowerShell penceresi acin (PATH yenilenir)
    echo   2. Kullanici Scripts klasorunu PATH'e ekleyin:
    !PYTHON! -c "import site; print('  ' + site.getusersitepackages().replace('site-packages','Scripts'))"
    echo.
) else (
    for /f "tokens=*" %%V in ('analyzer4pg --version 2^>^&1') do (
        echo [BASARI] analyzer4pg kuruldu: %%V
    )
)

REM ---- Kullanım örnekleri ----
echo.
echo ==========================================
echo   Kullanim Ornekleri
echo ==========================================
echo.
echo   Tek sorgu analizi:
echo   analyzer4pg analyze -H localhost -d mydb -U postgres ^
echo       -q "SELECT * FROM orders WHERE customer_id = 5"
echo.
echo   Dosyadan SQL okuma:
echo   analyzer4pg analyze -H localhost -d mydb -U postgres -f sorgu.sql
echo.
echo   Interaktif mod:
echo   analyzer4pg repl -H localhost -d mydb -U postgres
echo.
echo   EXPLAIN ANALYZE olmadan (sorgu calistirilmaz):
echo   analyzer4pg analyze --no-analyze -H localhost -d mydb -U postgres ^
echo       -q "SELECT * FROM orders"
echo.
echo Kurulum tamamlandi!
pause
exit /b 0

:install_error
echo.
echo [HATA] Kurulum basarisiz oldu.
echo.
echo Olasi cozumler:
echo   - Yonetici olarak calistirin (Yonetici olarak ac)
echo   - pip guncelleyin: python -m pip install --upgrade pip
echo   - PostgreSQL ODBC suruculeri kurulu mu kontrol edin
echo   - Antivirusunuzu gecici olarak devre disi birakin
pause
exit /b 1
