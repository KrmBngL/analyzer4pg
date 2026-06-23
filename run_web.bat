@echo off
setlocal EnableDelayedExpansion
REM analyzer4pg - Web Arayuzu Baslatici
REM Bu dosyayi cift tikla veya PowerShell'den calistir

cd /d "%~dp0"

REM analyzer4pg komutu var mi?
where analyzer4pg >nul 2>&1
if %errorlevel% == 0 (
    analyzer4pg web
    goto :end
)

REM Python ile direkt calistir
for %%P in (python3 python py) do (
    where %%P >nul 2>&1
    if !errorlevel! == 0 (
        %%P -m analyzer4pg web
        goto :end
    )
)

echo [HATA] Python bulunamadi. Lutfen once install.bat calistirin.
pause

:end
