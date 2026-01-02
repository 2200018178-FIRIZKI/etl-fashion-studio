# ETL Pipeline - Fashion Studio

ETL Pipeline sederhana untuk scraping data produk fashion dari website [Fashion Studio](https://fashion-studio.dicoding.dev/).

## 📋 Fitur

- **Extract**: Scraping data produk dari 50 halaman website
- **Transform**: Pembersihan dan transformasi data (konversi harga USD → IDR)
- **Load**: Menyimpan ke CSV dan PostgreSQL

## 🗂️ Struktur Project

```
Membangun ETL Pipeline/
├── utils/
│   ├── extract.py      # Modul scraping
│   ├── transform.py    # Modul transformasi data
│   └── load.py         # Modul penyimpanan data
├── tests/
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
├── main.py             # Orchestrator utama
├── requirements.txt    # Dependencies
├── Makefile           # Command shortcuts
└── products.csv       # Output data
```

## 🚀 Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Jalankan ETL Pipeline
```bash
# Quick test (2 halaman)
python main.py --pages 1 2

# Full run (50 halaman)
python main.py
```

### 3. Jalankan dengan PostgreSQL
```bash
python main.py --postgresql "postgresql://postgres:PASSWORD@localhost:5433/fashion_studio"
```

## 📊 Output Data

| Column | Type | Description |
|--------|------|-------------|
| Title | string | Nama produk |
| Price | float | Harga dalam IDR (1 USD = 16.000) |
| Rating | float | Rating produk (1.0-5.0) |
| Colors | int | Jumlah warna tersedia |
| Size | string | Ukuran (S, M, L, XL, XXL) |
| Gender | string | Target gender (Men, Women, Unisex) |
| timestamp | string | Waktu scraping |

## 🧪 Testing

```bash
# Jalankan semua test
pytest tests/ -v

# Dengan coverage report
pytest tests/ -v --cov=utils --cov-report=term-missing
```

**Coverage: 82%** (ADVANCED level)

## 📈 Hasil

- **Extracted**: 1000 records (50 halaman)
- **Cleaned**: 867 records (setelah transformasi)
- **Removed**: 133 records (Unknown Product, Price Unavailable)

## 🏆 Score Submission

| Kriteria | Level | Score |
|----------|-------|-------|
| Modular Code | ADVANCED | 4 pts |
| Repository | SKILLED | 3 pts |
| Unit Testing | ADVANCED | 4 pts |
| **Total** | **ADVANCED** | **11 pts** |

## 👤 Author

Data Engineer Shah Firizki Azmi - 2026
