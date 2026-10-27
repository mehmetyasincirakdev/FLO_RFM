# FLO RFM Customer Segmentation

FLO customer data üzerinde RFM (Recency, Frequency, Monetary) analizi yapar,
müşterileri segmentlere ayırır ve kampanya hedef kitlelerini CSV dosyaları
olarak üretir.

## Gereksinimler

- Python 3.10 veya üzeri
- `requirements.txt` içindeki paketler

Bağımlılıkları yükleyin:

```powershell
python -m pip install -r requirements.txt
```

## Analizi çalıştırma

Proje kök dizininde:

```powershell
python FLO_RFM.py
```

Varsayılan girdi `Datasets/flo_data_20k.csv`, varsayılan çıktı klasörü
`outputs/` dizinidir. Farklı dosya, çıktı klasörü veya analiz tarihi vermek için:

```powershell
python FLO_RFM.py --input Datasets/flo_data_20k.csv --output-dir outputs --analysis-date 2021-06-01
```

Analiz tarihi verilmezse veri kümesindeki en son sipariş tarihinden bir gün
sonrası kullanılır. Böylece recency değerleri negatif olmaz ve analiz tarihi
her veri kümesi için güncel kalır.

## Üretilen dosyalar

- `rfm_analysis.csv`: müşteri bazında RFM metrikleri, puanları ve segmentler
- `rfm_segment_summary.csv`: segment başına müşteri sayısı ve ortalama RFM
  metrikleri
- `yeni_marka_hedef_musteri_id.csv`: şampiyon ve sadık müşteriler arasından
  kadın kategorisine ilgi duyanlar
- `indirim_hedef_müşteri_ids.csv`: kaybetme riski olan, kaybedilmemesi gereken
  veya yeni müşteriler arasından erkek/çocuk kategorisine ilgi duyanlar

RFM puanları beşli sıralama dilimlerinden hesaplanır. Recency'de daha düşük
gün sayısı daha yüksek puan alır; eşit değerler aynı puanı korur. Girdi
beklenen sütunları içermiyorsa, müşteri kimlikleri tekrarlanıyorsa ya da
zorunlu tarih/sayı değerleri geçersizse analiz açık bir hata verir.

## Testler

```powershell
python -m unittest discover -s tests -v
```
