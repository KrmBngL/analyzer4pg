#!/usr/bin/env bash
# ============================================================
# analyzer4pg - Linux / macOS Kurulum Scripti
# Desteklenen: RHEL 8/9, Ubuntu 20.04+, macOS 12+
# ============================================================
set -e

BOLD="\033[1m"
GREEN="\033[0;32m"
YELLOW="\033[1;33m"
RED="\033[0;31m"
RESET="\033[0m"

info()    { echo -e "${GREEN}[INFO]${RESET} $*"; }
warn()    { echo -e "${YELLOW}[WARN]${RESET} $*"; }
error()   { echo -e "${RED}[ERROR]${RESET} $*" >&2; exit 1; }
header()  { echo -e "\n${BOLD}$*${RESET}"; }

header "=== analyzer4pg Kurulum ==="

# ---- Python kontrolü ----
PYTHON=""
for cmd in python3.12 python3.11 python3.10 python3.9 python3.8 python3; do
    if command -v "$cmd" &>/dev/null; then
        PY_VER=$("$cmd" -c "import sys; print(sys.version_info[:2])" 2>/dev/null)
        if "$cmd" -c "import sys; sys.exit(0 if sys.version_info >= (3,8) else 1)" 2>/dev/null; then
            PYTHON="$cmd"
            break
        fi
    fi
done

if [ -z "$PYTHON" ]; then
    error "Python 3.8+ bulunamadı.\n\
  RHEL/CentOS: sudo dnf install python3.11\n\
  Ubuntu:      sudo apt install python3.11\n\
  macOS:       brew install python@3.11"
fi

PY_VERSION=$("$PYTHON" --version 2>&1)
info "Python bulundu: $PY_VERSION ($PYTHON)"

# ---- pip kontrolü ----
if ! "$PYTHON" -m pip --version &>/dev/null; then
    warn "pip bulunamadı, kuruluyor..."

    # Önce ensurepip dene (dnf/apt gerekmez, Python'a gömülüdür)
    if "$PYTHON" -m ensurepip --upgrade &>/dev/null 2>&1; then
        info "pip ensurepip ile kuruldu."
    elif command -v apt-get &>/dev/null; then
        sudo apt-get install -y python3-pip
    elif command -v dnf &>/dev/null; then
        # Bozuk/eski repolar varsa devre dışı bırakarak kur (pgdg13 gibi)
        sudo dnf install -y python3-pip \
            --disablerepo='pgdg*' \
            --skip-broken 2>/dev/null \
        || {
            warn "dnf ile pip kurulamadı, get-pip.py deneniyor..."
            "$PYTHON" -m ensurepip --upgrade 2>/dev/null \
            || "$PYTHON" - <<'GETPIP'
import urllib.request, tempfile, os, subprocess, sys
url = 'https://bootstrap.pypa.io/get-pip.py'
tmp = tempfile.mktemp(suffix='.py')
print(f"get-pip.py indiriliyor: {url}")
urllib.request.urlretrieve(url, tmp)
subprocess.check_call([sys.executable, tmp, '--quiet'])
os.remove(tmp)
GETPIP
        }
    else
        "$PYTHON" - <<'GETPIP'
import urllib.request, tempfile, os, subprocess, sys
url = 'https://bootstrap.pypa.io/get-pip.py'
tmp = tempfile.mktemp(suffix='.py')
print(f"get-pip.py indiriliyor: {url}")
urllib.request.urlretrieve(url, tmp)
subprocess.check_call([sys.executable, tmp, '--quiet'])
os.remove(tmp)
GETPIP
    fi
fi

# Son kontrol
if ! "$PYTHON" -m pip --version &>/dev/null; then
    error "pip kurulamadı. Manuel kurulum:\n  $PYTHON -m ensurepip --upgrade\n  veya: curl -sS https://bootstrap.pypa.io/get-pip.py | $PYTHON"
fi

info "pip sürümü: $($PYTHON -m pip --version)"

# ---- libpq kontrolü (psycopg2 için) ----
if ! python3 -c "import psycopg2" &>/dev/null; then
    if ! pkg-config --exists libpq 2>/dev/null && ! ldconfig -p 2>/dev/null | grep -q libpq; then
        warn "libpq kütüphanesi bulunamadı. psycopg2-binary kullanılacak (statik link)."
        warn "Eğer sorun yaşarsanız:"
        warn "  RHEL/CentOS: sudo dnf install libpq-devel"
        warn "  Ubuntu:      sudo apt-get install libpq-dev"
    fi
fi

# ---- Kurulum yöntemi seçimi ----
header "Kurulum yöntemi"
echo "  1) Kullanıcı dizinine kur (~/.local) — önerilen, sudo gerekmez"
echo "  2) Sanal ortama (venv) kur"
echo "  3) Sistem geneline kur (sudo gerekir)"
echo ""
read -rp "Seçim [1]: " CHOICE
CHOICE="${CHOICE:-1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

case "$CHOICE" in
    1)
        info "Kullanıcı dizinine kuruluyor..."
        "$PYTHON" -m pip install --user -e "$SCRIPT_DIR"
        INSTALL_PATH="$HOME/.local/bin"
        ;;
    2)
        VENV_DIR="${VENV_DIR:-$SCRIPT_DIR/.venv}"
        info "Sanal ortam oluşturuluyor: $VENV_DIR"
        "$PYTHON" -m venv "$VENV_DIR"
        "$VENV_DIR/bin/pip" install --upgrade pip
        "$VENV_DIR/bin/pip" install -e "$SCRIPT_DIR"
        INSTALL_PATH="$VENV_DIR/bin"
        echo ""
        info "Sanal ortamı aktive etmek için:"
        info "  source $VENV_DIR/bin/activate"
        ;;
    3)
        info "Sistem geneline kuruluyor (sudo gerekebilir)..."
        sudo "$PYTHON" -m pip install -e "$SCRIPT_DIR"
        INSTALL_PATH="/usr/local/bin"
        ;;
    *)
        error "Geçersiz seçim: $CHOICE"
        ;;
esac

# ---- PATH kontrolü ----
echo ""
if command -v analyzer4pg &>/dev/null; then
    info "analyzer4pg başarıyla kuruldu!"
    info "Sürüm: $(analyzer4pg --version)"
else
    warn "analyzer4pg PATH'te bulunamadı."
    warn "Şunu ~/.bashrc veya ~/.zshrc dosyanıza ekleyin:"
    warn "  export PATH=\"$INSTALL_PATH:\$PATH\""
    warn "Sonra çalıştırın: source ~/.bashrc"
fi

# ---- Kullanım örneği ----
echo ""
header "=== Kullanım Örnekleri ==="
echo ""
echo "  # Tek sorgu analizi:"
echo "  analyzer4pg analyze -H localhost -d mydb -U postgres \\"
echo "      -q \"SELECT * FROM orders WHERE customer_id = 5\""
echo ""
echo "  # Dosyadan SQL okuma:"
echo "  analyzer4pg analyze -H localhost -d mydb -U postgres -f sorgu.sql"
echo ""
echo "  # İnteraktif mod:"
echo "  analyzer4pg repl -H localhost -d mydb -U postgres"
echo ""
echo "  # EXPLAIN ANALYZE olmadan (sadece plan):"
echo "  analyzer4pg analyze --no-analyze -H localhost -d mydb -U postgres \\"
echo "      -q \"SELECT * FROM orders\""
echo ""
info "Kurulum tamamlandı. İyi analizler! 🐘"
