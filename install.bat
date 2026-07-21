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

REM ---- PATH güncelleme (kullanıcı kurulumu için) ----
if "!INSTALL_TYPE!"=="user" (
    REM Dogru formul: {userbase}\PythonXY\Scripts (site-packages ile KARDES klasor,
    REM iki degil TEK seviye yukarida - onceki surumdeki hata buradaydi)
    for /f "tokens=*" %%S in ('!PYTHON! -c "import site, sys, os; print(os.path.join(site.getuserbase(), 'Python%%d%%d' %% sys.version_info[:2], 'Scripts'))" 2^>^&1') do set USER_SCRIPTS=%%S

    if not exist "!USER_SCRIPTS!\analyzer4pg.exe" (
        echo [BILGI] Beklenen konumda bulunamadi, taraniyor: !USER_SCRIPTS!
        for /f "delims=" %%F in ('dir /s /b "%APPDATA%\Python\analyzer4pg.exe" 2^>nul') do set USER_SCRIPTS=%%~dpF
        if defined USER_SCRIPTS if "!USER_SCRIPTS:~-1!"=="\" set USER_SCRIPTS=!USER_SCRIPTS:~0,-1!
    )

    if not "!USER_SCRIPTS!"=="" (
        echo [BILGI] Python Scripts klasoru PATH'e ekleniyor: !USER_SCRIPTS!
        setx PATH "!PATH!;!USER_SCRIPTS!" >nul 2>&1
        if errorlevel 1 (
            echo [UYARI] PATH otomatik eklenemedi. Manuel ekleyin: !USER_SCRIPTS!
        ) else (
            echo [BASARI] PATH guncellendi. Yeni terminal acin.
        )
        if not exist "!USER_SCRIPTS!\analyzer4pg.exe" (
            echo [UYARI] analyzer4pg.exe bu klasorde bulunamadi, PATH yine de eklendi.
            echo         Sorun devam ederse: !PYTHON! -m analyzer4pg web
        )
    ) else (
        echo [UYARI] Scripts klasoru tespit edilemedi. Dogrudan calistirin:
        echo         !PYTHON! -m analyzer4pg web
    )
)

REM ---- Kurulum kontrolü ----
echo.
analyzer4pg --version >nul 2>&1
if errorlevel 1 (
    echo [UYARI] analyzer4pg komutu bu terminalde bulunamadi.
    echo [BILGI] Yeni bir cmd/PowerShell penceresi acin ve tekrar deneyin.
    echo.
    echo Alternatif - Dogrudan Python ile calistirin:
    echo   !PYTHON! -m analyzer4pg web
    echo   !PYTHON! -m analyzer4pg analyze -H localhost -d mydb -U postgres -q "SELECT ..."
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
echo   Web arayuzu (tarayici otomatik acilir):
echo   analyzer4pg web
echo   veya: !PYTHON! -m analyzer4pg web
echo.
echo   Interaktif mod:
echo   analyzer4pg repl -H localhost -d mydb -U postgres
echo.
echo   Tek sorgu analizi:
echo   analyzer4pg analyze -H localhost -d mydb -U postgres ^
echo       -q "SELECT * FROM mytable WHERE id = 1"
echo.
echo Kurulum tamamlandi!
echo.
echo NOT: Eger 'analyzer4pg' komutu taninmiyorsa:
echo   1. Bu terminali kapatin ve yeni bir terminal acin
echo   2. Veya: !PYTHON! -m analyzer4pg web
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
