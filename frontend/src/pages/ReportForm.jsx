import { useMemo, useState } from "react";
import { Camera, CheckCircle2, Crosshair, Loader2, Send, Trash2 } from "lucide-react";
import { submitReport } from "../api";
import ReportMap from "../components/ReportMap";

const initialForm = {
  reporter_name: "",
  email: "",
  road_name: "",
  district: "",
  village: "",
  description: "",
};

export default function ReportForm() {
  const [form, setForm] = useState(initialForm);
  const [photos, setPhotos] = useState([]);
  const [position, setPosition] = useState(null);
  const [loadingLocation, setLoadingLocation] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [message, setMessage] = useState("");
  const [result, setResult] = useState(null);

  const previews = useMemo(
    () => photos.map((file) => ({ file, url: URL.createObjectURL(file) })),
    [photos],
  );

  function updateField(event) {
    setForm((current) => ({ ...current, [event.target.name]: event.target.value }));
  }

  function handlePhotos(event) {
    const selected = Array.from(event.target.files || []);
    const next = [...photos, ...selected].slice(0, 5);
    setPhotos(next);
    if (selected.length + photos.length > 5) {
      setMessage("Upload dibatasi maksimal 5 foto.");
    }
  }

  function removePhoto(index) {
    setPhotos((current) => current.filter((_, itemIndex) => itemIndex !== index));
  }

  function getLocation() {
    setMessage("");
    if (!navigator.geolocation) {
      setMessage("Browser tidak mendukung Geolocation API.");
      return;
    }

    setLoadingLocation(true);
    navigator.geolocation.getCurrentPosition(
      (geo) => {
        setPosition([geo.coords.latitude, geo.coords.longitude]);
        setLoadingLocation(false);
      },
      () => {
        setMessage("Gagal mengambil lokasi. Pastikan izin lokasi sudah diberikan.");
        setLoadingLocation(false);
      },
      { enableHighAccuracy: true, timeout: 12000 },
    );
  }

  async function handleSubmit(event) {
    event.preventDefault();
    setMessage("");
    setResult(null);

    if (!position) {
      setMessage("Ambil lokasi GPS terlebih dahulu.");
      return;
    }
    if (photos.length === 0) {
      setMessage("Tambahkan minimal 1 foto jalan rusak.");
      return;
    }

    const payload = new FormData();
    Object.entries(form).forEach(([key, value]) => {
      if (value) payload.append(key, value);
    });
    payload.append("latitude", position[0]);
    payload.append("longitude", position[1]);
    photos.forEach((photo) => payload.append("photos", photo));

    setSubmitting(true);
    try {
      const response = await submitReport(payload);
      setResult(response);
      setForm(initialForm);
      setPhotos([]);
      setPosition(null);
    } catch (error) {
      setMessage(error.message);
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <section className="mx-auto max-w-7xl px-4 py-8 sm:px-6 lg:px-8">
      <div className="mb-6">
        <p className="text-sm font-semibold uppercase tracking-wide text-civic-600">Form Pelaporan</p>
        <h1 className="mt-2 text-3xl font-bold text-civic-900">Laporkan Jalan Rusak</h1>
        <p className="mt-2 max-w-3xl text-slate-600">
          Kirim informasi jalan, foto, dan koordinat GPS agar laporan dapat diverifikasi lebih cepat.
        </p>
      </div>

      <form onSubmit={handleSubmit} className="grid gap-6 lg:grid-cols-[1fr_0.95fr]">
        <div className="space-y-6">
          <div className="panel p-5">
            <h2 className="text-lg font-bold text-slate-900">Informasi Pelapor</h2>
            <div className="mt-4 grid gap-4 sm:grid-cols-2">
              <label className="grid gap-2">
                <span className="label">Nama</span>
                <input className="input" name="reporter_name" value={form.reporter_name} onChange={updateField} required />
              </label>
              <label className="grid gap-2">
                <span className="label">Email (opsional)</span>
                <input className="input" type="email" name="email" value={form.email} onChange={updateField} />
              </label>
            </div>
          </div>

          <div className="panel p-5">
            <h2 className="text-lg font-bold text-slate-900">Informasi Jalan</h2>
            <div className="mt-4 grid gap-4 sm:grid-cols-3">
              <label className="grid gap-2 sm:col-span-3">
                <span className="label">Nama Jalan</span>
                <input className="input" name="road_name" value={form.road_name} onChange={updateField} placeholder="Jl. Ahmad Yani" required />
              </label>
              <label className="grid gap-2">
                <span className="label">Kecamatan</span>
                <input className="input" name="district" value={form.district} onChange={updateField} required />
              </label>
              <label className="grid gap-2">
                <span className="label">Kelurahan</span>
                <input className="input" name="village" value={form.village} onChange={updateField} required />
              </label>
            </div>
            <label className="mt-4 grid gap-2">
              <span className="label">Deskripsi</span>
              <textarea
                className="input min-h-32 resize-y"
                name="description"
                value={form.description}
                onChange={updateField}
                placeholder="Jalan berlubang cukup besar dan sering menyebabkan kendaraan menghindar."
                required
              />
            </label>
          </div>

          <div className="panel p-5">
            <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
              <div>
                <h2 className="text-lg font-bold text-slate-900">Upload Foto</h2>
                <p className="text-sm text-slate-500">Maksimal 5 foto.</p>
              </div>
              <label className="btn-secondary cursor-pointer">
                <Camera size={18} />
                Pilih Foto
                <input className="hidden" type="file" accept="image/*" multiple onChange={handlePhotos} />
              </label>
            </div>
            {previews.length > 0 && (
              <div className="mt-4 grid grid-cols-2 gap-3 sm:grid-cols-5">
                {previews.map((preview, index) => (
                  <div key={preview.url} className="relative aspect-square overflow-hidden rounded-md border border-slate-200">
                    <img src={preview.url} alt={`Preview ${index + 1}`} className="h-full w-full object-cover" />
                    <button
                      type="button"
                      className="absolute right-2 top-2 grid h-8 w-8 place-items-center rounded-md bg-white/90 text-red-600 shadow-sm"
                      onClick={() => removePhoto(index)}
                      aria-label="Hapus foto"
                    >
                      <Trash2 size={16} />
                    </button>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>

        <div className="space-y-6">
          <div className="panel p-5">
            <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
              <div>
                <h2 className="text-lg font-bold text-slate-900">Lokasi GPS</h2>
                <p className="text-sm text-slate-500">Marker dapat digeser setelah lokasi muncul.</p>
              </div>
              <button type="button" className="btn-primary" onClick={getLocation} disabled={loadingLocation}>
                {loadingLocation ? <Loader2 className="animate-spin" size={18} /> : <Crosshair size={18} />}
                Ambil Lokasi Saat Ini
              </button>
            </div>
            <div className="mt-4 grid gap-3 sm:grid-cols-2">
              <div className="rounded-md border border-slate-200 bg-slate-50 p-3">
                <p className="text-xs font-semibold text-slate-500">Latitude</p>
                <p className="mt-1 font-mono text-sm text-slate-900">{position ? position[0].toFixed(6) : "-"}</p>
              </div>
              <div className="rounded-md border border-slate-200 bg-slate-50 p-3">
                <p className="text-xs font-semibold text-slate-500">Longitude</p>
                <p className="mt-1 font-mono text-sm text-slate-900">{position ? position[1].toFixed(6) : "-"}</p>
              </div>
            </div>
            <div className="mt-4">
              <ReportMap position={position} onPositionChange={setPosition} />
            </div>
          </div>

          {message && <div className="rounded-md border border-amber-200 bg-amber-50 p-4 text-sm font-medium text-amber-800">{message}</div>}

          {result && (
            <div className="rounded-lg border border-emerald-200 bg-emerald-50 p-5 text-emerald-900">
              <div className="flex items-center gap-2 font-bold">
                <CheckCircle2 size={20} />
                Laporan berhasil dikirim
              </div>
              <pre className="mt-3 overflow-auto rounded-md bg-white p-3 text-xs text-slate-800">
                {JSON.stringify(result, null, 2)}
              </pre>
            </div>
          )}

          <button type="submit" className="btn-primary w-full" disabled={submitting}>
            {submitting ? <Loader2 className="animate-spin" size={18} /> : <Send size={18} />}
            Kirim Laporan
          </button>
        </div>
      </form>
    </section>
  );
}
