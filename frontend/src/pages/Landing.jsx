import { Link } from "react-router-dom";
import { ArrowRight, Camera, ClipboardList, Map, ShieldCheck } from "lucide-react";
import { useEffect, useState } from "react";
import { fetchStats } from "../api";
import StatCard from "../components/StatCard";

export default function Landing() {
  const [stats, setStats] = useState({
    total_reports: 128,
    today_reports: 14,
    top_district: "Wonokromo",
  });

  useEffect(() => {
    fetchStats().then(setStats).catch(() => {});
  }, []);

  return (
    <div>
      <section className="relative overflow-hidden bg-white">
        <div className="absolute inset-0 bg-[linear-gradient(115deg,#eff8ff_0%,#ffffff_48%,#dbeefe_100%)]" />
        <div className="relative mx-auto grid min-h-[78vh] max-w-7xl items-center gap-10 px-4 py-12 sm:px-6 lg:grid-cols-[1.05fr_0.95fr] lg:px-8">
          <div>
            <p className="mb-4 inline-flex items-center gap-2 rounded-md bg-civic-50 px-3 py-2 text-sm font-semibold text-civic-700">
              <ShieldCheck size={17} />
              Smart City Road Monitoring
            </p>
            <h1 className="max-w-4xl text-4xl font-bold leading-tight text-civic-900 sm:text-5xl lg:text-6xl">
              Smart Road Intelligence System Surabaya
            </h1>
            <p className="mt-5 max-w-2xl text-base leading-7 text-slate-600 sm:text-lg">
              Platform crowdsourcing untuk mengumpulkan laporan jalan rusak dari masyarakat, memetakan lokasi GPS,
              dan membantu prioritas penanganan berbasis deteksi AI.
            </p>
            <div className="mt-8 flex flex-col gap-3 sm:flex-row">
              <Link to="/report" className="btn-primary">
                Laporkan Jalan Rusak
                <ArrowRight size={18} />
              </Link>
              <Link to="/admin" className="btn-secondary">
                Lihat Dashboard
              </Link>
            </div>
          </div>

          <div className="panel overflow-hidden">
            <div className="border-b border-slate-200 bg-civic-900 px-5 py-4 text-white">
              <p className="text-sm font-semibold">Live Road Intelligence</p>
              <p className="text-xs text-civic-100">Surabaya city operations view</p>
            </div>
            <div className="grid gap-4 p-5">
              <div className="grid grid-cols-3 gap-3">
                <div className="rounded-md bg-civic-50 p-4">
                  <p className="text-xs font-medium text-slate-500">Total</p>
                  <p className="mt-2 text-2xl font-bold text-civic-900">{stats.total_reports}</p>
                </div>
                <div className="rounded-md bg-emerald-50 p-4">
                  <p className="text-xs font-medium text-slate-500">Hari Ini</p>
                  <p className="mt-2 text-2xl font-bold text-emerald-700">{stats.today_reports}</p>
                </div>
                <div className="rounded-md bg-amber-50 p-4">
                  <p className="text-xs font-medium text-slate-500">AI Avg</p>
                  <p className="mt-2 text-2xl font-bold text-amber-700">82</p>
                </div>
              </div>
              <div className="relative h-72 overflow-hidden rounded-lg bg-slate-100">
                <div className="absolute inset-0 bg-[linear-gradient(135deg,#e9f5ff,#ffffff)]" />
                <div className="absolute inset-0 opacity-60 [background-image:linear-gradient(#cbdff1_1px,transparent_1px),linear-gradient(90deg,#cbdff1_1px,transparent_1px)] [background-size:34px_34px]" />
                <div className="absolute left-8 top-8 h-20 w-36 rounded-md border border-civic-200 bg-white/85 p-3 shadow-sm">
                  <p className="text-xs font-semibold text-slate-500">Kecamatan Tertinggi</p>
                  <p className="mt-2 text-lg font-bold text-civic-900">{stats.top_district || "-"}</p>
                </div>
                <div className="absolute bottom-6 right-6 grid h-24 w-24 place-items-center rounded-full border-8 border-civic-100 bg-white text-center shadow-sm">
                  <div>
                    <p className="text-xl font-bold text-civic-700">GPS</p>
                    <p className="text-xs text-slate-500">Ready</p>
                  </div>
                </div>
                <div className="absolute bottom-10 left-10 h-3 w-3 rounded-full bg-red-500 shadow-[0_0_0_10px_rgba(239,68,68,0.16)]" />
                <div className="absolute right-32 top-24 h-3 w-3 rounded-full bg-amber-500 shadow-[0_0_0_10px_rgba(245,158,11,0.16)]" />
              </div>
            </div>
          </div>
        </div>
      </section>

      <section className="mx-auto max-w-7xl px-4 py-10 sm:px-6 lg:px-8">
        <div className="grid gap-4 md:grid-cols-3">
          <StatCard icon={ClipboardList} label="Laporan Masuk" value={stats.total_reports} />
          <StatCard icon={Camera} label="Laporan Hari Ini" value={stats.today_reports} tone="green" />
          <StatCard icon={Map} label="Kecamatan Teratas" value={stats.top_district || "-"} tone="amber" />
        </div>
      </section>
    </div>
  );
}
