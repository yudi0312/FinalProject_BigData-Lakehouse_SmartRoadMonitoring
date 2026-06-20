"""
SRIS Phase 2 — NLP Extractor (versi diperkuat)
================================================
Modul NLP terpisah supaya mudah di-unit test dan di-reuse
baik oleh scraper Kafka maupun pipeline lain.

Perbaikan dari versi sebelumnya:
1. Multi-road detection — satu berita bisa nyebut > 1 jalan
2. Severity sekarang punya skor numerik (0-100), bukan cuma label
3. District matching lebih toleran (regex word-boundary + alias "Kec.")
4. Sentiment pakai bobot + deteksi negasi sederhana ("tidak rusak" != negative)
5. Confidence score per field — supaya hasil low-confidence bisa di-flag untuk review manual
6. Setiap road_name yang terdeteksi dipasangkan dengan damage_type & severity
   yang konteksnya plg dekat (windowed proximity), bukan damage_type global
"""

import re
from dataclasses import dataclass, field
from typing import Optional


# ─────────────────────────────────────────────
# KEYWORD BANKS
# ─────────────────────────────────────────────

KECAMATAN_SURABAYA = [
    "Wonokromo", "Wonocolo", "Wiyung", "Wage", "Tandes", "Tegalsari",
    "Tambaksari", "Sukolilo", "Sukomanunggal", "Simokerto", "Semampir",
    "Sawahan", "Rungkut", "Pakal", "Pabean Cantian", "Mulyorejo",
    "Lakarsantri", "Krembangan", "Kenjeran", "Karang Pilang", "Jambangan",
    "Gunung Anyar", "Gayungan", "Genteng", "Gubeng", "Dukuh Pakis",
    "Bubutan", "Bulak", "Benowo", "Asemrowo", "Waru", "Warugunung",
    "Menanggal", "Medokan Ayu", "Klampis Ngasem",
]

# Kecamatan diurutkan dari nama terpanjang dulu, supaya "Pabean Cantian"
# tidak ke-cut jadi "Pabean" doang saat regex match pertama ditemukan.
_KECAMATAN_SORTED = sorted(KECAMATAN_SURABAYA, key=len, reverse=True)

ROAD_STOP_WORDS = (
    r"rusak|berlubang|hancur|parah|ambles|amblas|bergelombang|akhirnya|selesai|"
    r"di|yang|dan|kawasan|warga|sudah|telah|masih|kembali|menjadi|terjadi|"
    r"akibat|karena|sejak|hingga|serta|juga"
)

# Pola nama jalan. Grup 1 = nama jalan.
# NEGATIVE LOOKAHEAD (?!Jalan\b|Jl\.) di tiap kata kedua-dst mencegah regex
# "menelan" prefix "Jalan"/"Jl." berikutnya kalau ada 2 penyebutan jalan
# yang bertumpuk tanpa pemisah (contoh: judul "...Jalan X" + body "Jalan X...").
ROAD_PATTERNS = [
    rf"(?:Jalan|Jl\.)\s+((?!Jalan\b|Jl\.)[A-Z][A-Za-z\.]+"
    rf"(?:\s+(?!Jalan\b|Jl\.)[A-Z][A-Za-z\.]+){{0,3}})"
    rf"(?=\s+(?:{ROAD_STOP_WORDS})|[,\.]|$)",
]

DAMAGE_KEYWORDS = {
    "pothole":         ["berlubang", "lubang", "jalan bolong", "lobang"],
    "crack":           ["retak", "retakan", "pecah", "terbelah"],
    "alligator_crack": ["bergelombang", "retak buaya", "kulit buaya"],
    "subsidence":      ["ambles", "amblas", "turun", "terbenam"],
    "erosion":         ["tergerus", "erosi", "longsor", "terkikis"],
    "general":         ["rusak", "hancur", "tidak layak"],
}

# Skor severity numerik (0-100) dipasangkan dengan label kategorikal,
# supaya bisa langsung dipakai di formula RHI/RPS tanpa mapping ulang.
SEVERITY_KEYWORDS = {
    "critical": (90, ["parah sekali", "sangat parah", "hancur total",
                       "tidak bisa dilalui", "berbahaya", "maut", "fatal",
                       "memakan korban"]),
    "high":     (70, ["parah", "rusak berat", "berlubang besar",
                       "cukup parah", "dalam dan besar"]),
    "medium":   (45, ["rusak sedang", "berlubang", "cukup rusak",
                       "mulai rusak", "bergelombang"]),
    "low":      (20, ["rusak ringan", "sedikit rusak", "retak kecil",
                       "agak rusak", "retak halus"]),
}

COMPLAINT_PATTERNS = [
    r"(\d+)\s*(?:warga|masyarakat|orang|laporan|pengaduan)",
]
COMPLAINT_APPROX = {
    "ratusan":  150,
    "puluhan":  30,
    "banyak":   20,
    "sejumlah": 10,
    "beberapa": 5,
}

SENTIMENT_POSITIVE = [
    "diperbaiki", "sudah ditangani", "selesai diperbaiki", "telah direnovasi",
    "sudah bagus", "mulus", "baik", "terima kasih", "apresiasi",
    "ditambal", "pengaspalan selesai",
]
SENTIMENT_NEGATIVE = [
    "rusak", "berlubang", "berbahaya", "parah", "keluhan", "protes",
    "mengeluh", "minta perbaikan", "belum ditangani", "dibiarkan",
    "tidak aman", "menelan korban", "kecelakaan",
]
# Kata negasi yang membatalkan makna kata sesudahnya (window pendek)
NEGATION_WORDS = ["tidak", "belum", "bukan", "tanpa"]


# ─────────────────────────────────────────────
# DATA CLASSES
# ─────────────────────────────────────────────

@dataclass
class RoadMention:
    """Satu jalan yang disebut dalam berita, dengan konteks lokalnya sendiri."""
    road_name:        str
    district:         Optional[str] = None
    damage_type:       Optional[str] = None
    severity_level:    Optional[str] = None
    severity_score:    Optional[int] = None     # 0-100, untuk feature engineering
    confidence:        float = 0.0              # 0.0-1.0


@dataclass
class ExtractionResult:
    """Hasil ekstraksi NLP lengkap untuk satu artikel."""
    roads:            list[RoadMention] = field(default_factory=list)
    complaint_count:  Optional[int] = None
    sentiment:        str = "neutral"
    sentiment_score:  float = 0.0               # -1.0 (negatif) .. +1.0 (positif)
    extraction_confidence: float = 0.0          # overall confidence skor ekstraksi

    # Properti turunan untuk backward-compat (kalau hanya butuh 1 jalan utama)
    @property
    def primary_road(self) -> Optional[RoadMention]:
        if not self.roads:
            return None
        # Jalan dengan confidence tertinggi dianggap "utama"
        return max(self.roads, key=lambda r: r.confidence)


# ─────────────────────────────────────────────
# NLP EXTRACTOR
# ─────────────────────────────────────────────

class NLPExtractor:
    """
    Rule-based NLP extractor untuk berita jalan rusak.
    Tidak butuh model ML — cepat dan tidak butuh GPU, cocok untuk
    pipeline streaming real-time (dipanggil per-artikel di Kafka producer).
    """

    WINDOW_CHARS = 80  # radius karakter di sekitar nama jalan untuk cari damage/severity lokal

    # ---------- Road detection (multi) ----------
    def extract_roads(self, text: str) -> list[str]:
        """Ambil SEMUA nama jalan unik yang disebut, bukan cuma yang pertama."""
        found = []
        seen = set()
        for pattern in ROAD_PATTERNS:
            for m in re.finditer(pattern, text):
                name = m.group(1).strip().rstrip(".,")
                key = name.lower()
                if 3 < len(name) < 40 and key not in seen:
                    seen.add(key)
                    found.append((name, m.start()))
        found.sort(key=lambda x: x[1])

        # Safety net: kalau ada dua nama yang salah satunya substring dari yang
        # lain (misal "Kertajaya" vs "Kertajaya Indah"), simpan yang lebih panjang/spesifik saja.
        names = [n for n, _ in found]
        deduped = []
        for name in names:
            if any(name.lower() != other.lower() and name.lower() in other.lower()
                   for other in names):
                continue  # nama ini adalah substring dari nama lain yang lebih lengkap, skip
            deduped.append(name)
        return deduped

    def _local_window(self, text: str, road_name: str) -> str:
        """Ambil potongan teks di sekitar nama jalan untuk analisis kontekstual."""
        idx = text.find(road_name)
        if idx == -1:
            return text
        start = max(0, idx - self.WINDOW_CHARS)
        end = min(len(text), idx + len(road_name) + self.WINDOW_CHARS)
        return text[start:end]

    # ---------- District ----------
    def extract_district(self, text: str) -> Optional[str]:
        text_lower = text.lower()
        for kec in _KECAMATAN_SORTED:
            # word-boundary match, toleran terhadap prefix "Kec." / "kecamatan"
            pattern = rf"(?:kec\.?\s*|kecamatan\s+)?\b{re.escape(kec.lower())}\b"
            if re.search(pattern, text_lower):
                return kec
        return None

    # ---------- Damage type ----------
    def extract_damage_type(self, text: str) -> tuple[Optional[str], float]:
        text_lower = text.lower()
        matches = []
        for dtype, keywords in DAMAGE_KEYWORDS.items():
            hits = sum(1 for kw in keywords if kw in text_lower)
            if hits:
                matches.append((dtype, hits))
        if not matches:
            return None, 0.0
        matches.sort(key=lambda x: x[1], reverse=True)
        best_type, hits = matches[0]
        confidence = min(1.0, 0.4 + 0.2 * hits)  # 1 hit=0.6, 2 hit=0.8, 3+ hit=1.0
        return best_type, confidence

    # ---------- Severity (kategorikal + skor numerik) ----------
    def extract_severity(self, text: str) -> tuple[Optional[str], Optional[int], float]:
        text_lower = text.lower()
        candidates = []
        for level, (score, keywords) in SEVERITY_KEYWORDS.items():
            hits = [kw for kw in keywords if kw in text_lower]
            if hits:
                candidates.append((level, score, len(hits)))

        if not candidates:
            # fallback lemah: indikasi umum kerusakan tanpa kata intensitas spesifik
            if "rusak" in text_lower or "berlubang" in text_lower:
                return "medium", 45, 0.3
            return None, None, 0.0

        # Pilih kandidat paling "parah" yang muncul (prioritas critical > high > ...)
        order = {"critical": 4, "high": 3, "medium": 2, "low": 1}
        candidates.sort(key=lambda c: (order[c[0]], c[2]), reverse=True)
        level, score, hit_count = candidates[0]
        confidence = min(1.0, 0.5 + 0.25 * hit_count)
        return level, score, confidence

    # ---------- Complaint count ----------
    def extract_complaint_count(self, text: str) -> tuple[Optional[int], float]:
        text_lower = text.lower()
        for pattern in COMPLAINT_PATTERNS:
            m = re.search(pattern, text_lower)
            if m:
                try:
                    return int(m.group(1)), 0.9  # angka eksplisit = confidence tinggi
                except (ValueError, IndexError):
                    pass
        for word, approx in COMPLAINT_APPROX.items():
            if word in text_lower:
                return approx, 0.4  # estimasi kata kuantitatif = confidence rendah
        return None, 0.0

    # ---------- Sentiment (dengan deteksi negasi sederhana) ----------
    def extract_sentiment(self, text: str) -> tuple[str, float]:
        text_lower = text.lower()
        words = re.findall(r"\w+", text_lower)

        pos_score = 0.0
        neg_score = 0.0

        for kw in SENTIMENT_POSITIVE:
            for m in re.finditer(re.escape(kw), text_lower):
                if self._is_negated(text_lower, m.start()):
                    neg_score += 1  # "tidak diperbaiki" -> jadi negatif
                else:
                    pos_score += 1

        for kw in SENTIMENT_NEGATIVE:
            for m in re.finditer(re.escape(kw), text_lower):
                if self._is_negated(text_lower, m.start()):
                    pos_score += 0.5  # "tidak rusak" -> melemahkan negatif (bukan langsung positif kuat)
                else:
                    neg_score += 1

        total = pos_score + neg_score
        if total == 0:
            return "neutral", 0.0

        net = (pos_score - neg_score) / total  # -1..+1
        if net > 0.15:
            label = "positive"
        elif net < -0.15:
            label = "negative"
        else:
            label = "neutral"
        return label, round(net, 2)

    def _is_negated(self, text_lower: str, idx: int, window: int = 15) -> bool:
        """Cek apakah ada kata negasi tepat sebelum keyword (window karakter)."""
        start = max(0, idx - window)
        preceding = text_lower[start:idx]
        return any(neg in preceding for neg in NEGATION_WORDS)

    # ---------- Orkestrasi utama ----------
    def extract_all(self, title: str, content: str) -> ExtractionResult:
        combined = f"{title} {content}".strip()

        road_names = self.extract_roads(combined)
        global_district = self.extract_district(combined)

        roads: list[RoadMention] = []
        if road_names:
            for road_name in road_names:
                local_text = self._local_window(combined, road_name)

                local_district = self.extract_district(local_text) or global_district
                damage_type, dmg_conf = self.extract_damage_type(local_text)
                severity_level, severity_score, sev_conf = self.extract_severity(local_text)

                # confidence gabungan per-jalan: rata-rata sinyal yang berhasil diekstrak
                signals = [c for c in [dmg_conf, sev_conf] if c > 0]
                road_confidence = sum(signals) / len(signals) if signals else 0.2

                roads.append(RoadMention(
                    road_name=road_name,
                    district=local_district,
                    damage_type=damage_type,
                    severity_level=severity_level,
                    severity_score=severity_score,
                    confidence=round(road_confidence, 2),
                ))
        else:
            # Tidak ada nama jalan eksplisit — tetap simpan damage/severity level artikel,
            # supaya berita tidak hilang begitu saja (road_name=None, bisa di-review manual)
            damage_type, dmg_conf = self.extract_damage_type(combined)
            severity_level, severity_score, sev_conf = self.extract_severity(combined)
            if damage_type or severity_level:
                roads.append(RoadMention(
                    road_name="UNKNOWN",
                    district=global_district,
                    damage_type=damage_type,
                    severity_level=severity_level,
                    severity_score=severity_score,
                    confidence=round(min(dmg_conf, sev_conf) if dmg_conf and sev_conf
                                      else max(dmg_conf, sev_conf), 2),
                ))

        complaint_count, complaint_conf = self.extract_complaint_count(combined)
        sentiment, sentiment_score = self.extract_sentiment(combined)

        # Overall extraction confidence: rata-rata confidence semua sinyal yang ada
        all_confidences = [r.confidence for r in roads] + (
            [complaint_conf] if complaint_conf > 0 else []
        )
        overall_conf = (
            round(sum(all_confidences) / len(all_confidences), 2)
            if all_confidences else 0.0
        )

        return ExtractionResult(
            roads=roads,
            complaint_count=complaint_count,
            sentiment=sentiment,
            sentiment_score=sentiment_score,
            extraction_confidence=overall_conf,
        )


# ─────────────────────────────────────────────
# QUICK SELF-TEST
# ─────────────────────────────────────────────
if __name__ == "__main__":
    nlp = NLPExtractor()

    samples = [
        (
            "Warga Wonokromo Mengeluh Jalan Berlubang",
            "Warga mengeluh Jalan Ahmad Yani berlubang parah sejak 3 bulan, "
            "puluhan laporan masuk ke kelurahan Wonokromo. Selain itu Jalan Diponegoro "
            "di kawasan yang sama juga mulai rusak ringan."
        ),
        (
            "Perbaikan Jalan Selesai",
            "Perbaikan Jalan Mayjend Sungkono akhirnya selesai dikerjakan, warga "
            "apresiasi kerja cepat pemkot Surabaya. Jalan ini sebelumnya tidak rusak parah "
            "tapi tetap diperbaiki untuk pencegahan."
        ),
        (
            "Kecelakaan di Jalan Kertajaya",
            "Jalan Kertajaya bergelombang dan retak buaya, kondisinya sangat parah dan "
            "berbahaya, hingga menelan korban kecelakaan motor minggu lalu. Ratusan warga "
            "Gubeng sudah melapor."
        ),
    ]

    for title, content in samples:
        result = nlp.extract_all(title, content)
        print(f"\n{'='*70}")
        print(f"TITLE: {title}")
        print(f"sentiment={result.sentiment} (score={result.sentiment_score}) "
              f"| complaints={result.complaint_count} "
              f"| overall_confidence={result.extraction_confidence}")
        for r in result.roads:
            print(f"  - {r.road_name:25} district={str(r.district):15} "
                  f"damage={str(r.damage_type):16} severity={str(r.severity_level):10} "
                  f"score={str(r.severity_score):5} conf={r.confidence}")
