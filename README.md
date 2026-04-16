# analyzer4pg

**PostgreSQL Sorgu Analiz Aracı** — Oracle Tuning Advisor tarzında execution plan analizi, index önerileri ve sorgu yeniden yazım tavsiyeleri.

---

## Özellikler

| Özellik | Açıklama |
|---|---|
| **Execution Plan Görselleştirme** | EXPLAIN ANALYZE çıktısını renkli ağaç yapısında gösterir |
| **Plan Analizi** | Seq Scan, sort/hash spill, buffer miss, istatistik hataları tespiti |
| **Index Önerileri** | Hazır `CREATE INDEX CONCURRENTLY` ifadeleri üretir |
| **Kullanılmayan Index Tespiti** | `pg_stat_user_indexes` üzerinden sıfır kullanımlı indexler |
| **Sorgu Anti-Pattern Tespiti** | 15+ SQL anti-pattern kontrolü (SELECT *, NOT IN, LIKE '%...', vs.) |
| **Performans Skoru** | 0–100 arası skor + A/B/C/D/F notu |
| **İnteraktif REPL** | psql benzeri interaktif analiz kabuğu |
| **Çapraz Platform** | Windows, RHEL 8/9, Ubuntu 20.04+ |

---

## Kurulum

### Gereksinimler

- Python 3.8+
- PostgreSQL istemci kütüphanesi (psycopg2-binary ile otomatik gelir)

### Linux (RHEL 8/9 ve Ubuntu)

```bash
git clone https://github.com/krmbngl/analyzer4pg.git
cd analyzer4pg
chmod +x install.sh
./install.sh
```

### Windows

```bat
git clone https://github.com/krmbngl/analyzer4pg.git
cd analyzer4pg
install.bat
```

### pip ile Manuel Kurulum

```bash
pip install -e .
# veya kullanıcı dizinine:
pip install --user -e .
```

---

## Kullanım

### Tek Sorgu Analizi

```bash
analyzer4pg analyze \
    -H localhost -p 5432 \
    -d mydb -U postgres \
    -q "SELECT * FROM orders WHERE customer_id = 5"
```

### Dosyadan SQL Okuma

```bash
analyzer4pg analyze -H localhost -d mydb -U postgres -f sorgu.sql
```

### Stdin'den SQL Okuma

```bash
echo "SELECT * FROM orders WHERE amount > 100" | \
    analyzer4pg analyze -H localhost -d mydb -U postgres
```

### EXPLAIN Only (Sorguyu Çalıştırmadan)

```bash
analyzer4pg analyze --no-analyze \
    -H localhost -d mydb -U postgres \
    -q "SELECT * FROM large_table"
```

### İnteraktif REPL Modu

```bash
analyzer4pg repl -H localhost -d mydb -U postgres
```

REPL içinde:

```
analyzer4pg (mydb)> SELECT o.id, c.name
  ...                FROM orders o, customers c
  ...                WHERE o.customer_id = c.id
  ...                AND o.amount > 100;
```

REPL komutları: `\q` çıkış, `\h` yardım, `\c dbname` bağlantı değiştir, `\analyze on/off`

---

## Örnek Çıktı

```
╔══════════════════════════════════════════════════════════╗
║  analyzer4pg — PostgreSQL Sorgu Analiz Aracı             ║
║  Veritabanı: mydb   Sunucu: PostgreSQL 16.1              ║
╚══════════════════════════════════════════════════════════╝

─────────────────── Execution Plan ────────────────────────
  Planning: 0.312ms   Execution: 45.231ms   Total: 45.543ms

Execution Plan Tree:
├─ Hash Join  cost=0.00..1245.50  actual=45.2ms  rows=9500
│  ├─ Seq Scan on orders ⚠  cost=0..856  actual=38.1ms  rows=50000
│  │    Filter: (amount > 100)
│  └─ Hash
│       └─ Index Scan using customers_pkey  rows=5000

─────────────────── Plan Bulguları (2) ─────────────────────
  1. [WARNING] Sequential Scan on 'orders'
     50,000 satır taranıyor, Filter: (amount > 100)
     Öneri: amount sütununa index ekleyin.

─────────────────── Index Önerileri ────────────────────────
  1. [HIGH] public.orders (amount)
     CREATE INDEX CONCURRENTLY idx_orders_amount ON orders(amount);

─────────────────── Performans Özeti ───────────────────────
  Performans Skoru: 72/100 ████████████████████░░░░░░░░░░  Not: C
  — Orta — iyileştirme önerilir
```

---

## Tespit Edilen Anti-Pattern'ler

| # | Anti-Pattern | Öneri |
|---|---|---|
| 1 | `SELECT *` | Explicit sütun listesi |
| 2 | `LIKE '%metin'` | pg_trgm GIN index veya full-text search |
| 3 | `WHERE UPPER(col)` | Fonksiyonel index veya lowercase saklama |
| 4 | `NOT IN (SELECT ...)` | `NOT EXISTS` kullanımı |
| 5 | `OR col = X OR col = Y` | `IN (X, Y)` |
| 6 | `HAVING` (aggregate'siz) | `WHERE` ile değiştir |
| 7 | `SELECT DISTINCT` | JOIN düzelt veya `GROUP BY` |
| 8 | `UNION` (ALL değil) | `UNION ALL` |
| 9 | SELECT listesinde correlated subquery | `JOIN` ile değiştir |
| 10 | `ORDER BY RANDOM()` | `TABLESAMPLE` veya keyset |
| 11 | Büyük `OFFSET` değeri | Keyset pagination |
| 12 | `FROM t1, t2` (implicit join) | Explicit `JOIN ... ON` |
| 13 | Gereksiz inline view | Düzleştir veya CTE kullan |
| 14 | `COUNT(sütun)` vs `COUNT(*)` | NULL davranışını bil |
| 15 | Örtük tip dönüşümü | Doğru tip kullan |

---

## Bağlantı Seçenekleri

| Parametre | Kısa | Varsayılan | Açıklama |
|---|---|---|---|
| `--host` | `-H` | `localhost` | PostgreSQL sunucu adresi |
| `--port` | `-p` | `5432` | Port |
| `--dbname` | `-d` | `postgres` | Veritabanı adı |
| `--user` | `-U` | `postgres` | Kullanıcı adı |
| `--password` | `-W` | — | Parola (yoksa PGPASSWORD env veya prompt) |
| `--sslmode` | — | `prefer` | SSL modu |
| `--no-analyze` | — | — | Sadece EXPLAIN (sorguyu çalıştırmaz) |

---

## Lisans

MIT
