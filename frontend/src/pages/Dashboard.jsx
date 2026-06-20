import { useEffect, useMemo, useState } from "react";
import { Activity, AlertTriangle, BrainCircuit, CalendarDays, Gauge, MapPinned, RefreshCw, Route, Server } from "lucide-react";
import { API_BASE_URL, fetchReports, fetchStats, fetchHealthIndex, fetchPriorityScores } from "../api";
import ReportsMapView from "../components/ReportsMapView";
import StatCard from "../components/StatCard";

const fallbackReports = [
  {
    report_id: "R001",
    report_date: new Date().toISOString(),
    road_name: "Jl. Ahmad Yani",
    district: "Wonocolo",
    status: "Pending",
    severity_score: 100,
    confidence: 0.89,
    damage_type: "D40_Pothole",
    latitude: -7.335,
    longitude: 112.734,
    image_path: "",
  },
  {
    report_id: "R002",
    report_date: new Date().toISOString(),
    road_name: "Jl. Mayjen Sungkono",
    district: "Dukuh Pakis",
    status: "Verified",
    severity_score: 75,
    confidence: 0.88,
    damage_type: "D20_Alligator_Crack",
    latitude: -7.289,
    longitude: 112.713,
    image_path: "",
  },
  {
    report_id: "R003",
    report_date: new Date().toISOString(),
    road_name: "Jl. Kenjeran",
    district: "Bulak",
    status: "In Progress",
    severity_score: 50,
    confidence: 0.86,
    damage_type: "D10_Transverse_Crack",
    latitude: -7.241,
    longitude: 112.789,
    image_path: "",
  },
];

const statusStyle = {
  Pending: "bg-amber-50 text-amber-700 border-amber-200",
  Verified: "bg-blue-50 text-blue-700 border-blue-200",
  "In Progress": "bg-indigo-50 text-indigo-700 border-indigo-200",
  Resolved: "bg-emerald-50 text-emerald-700 border-emerald-200",
};

function firstImage(path) {
  if (!path) return "";
  const image = path.split(",")[0];
  return image.startsWith("http") ? image : `${API_BASE_URL}${image}`;
}

function BarChart({ title, data }) {
  const max = Math.max(...data.map((item) => item.value), 1);

  return (
    <div className="panel p-5">
      <h2 className="text-lg font-bold text-slate-900">{title}</h2>
      <div className="mt-5 space-y-4">
        {data.map((item) => (
          <div key={item.label}>
            <div className="mb-1 flex items-center justify-between text-sm">
              <span className="font-semibold text-slate-700">{item.label}</span>
              <span className="text-slate-500">{item.value}</span>
            </div>
            <div className="h-3 overflow-hidden rounded-full bg-slate-100">
              <div className="h-full rounded-full bg-civic-600" style={{ width: `${(item.value / max) * 100}%` }} />
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function formatConfidence(value) {
  if (value === null || value === undefined || value === "") return "-";
  return `${Math.round(Number(value) * 100)}%`;
}

export default function Dashboard() {
  const [stats, setStats] = useState({
    total_reports: 0,
    today_reports: 0,
    top_district: "-",
    total_detected_damage: 0,
    average_severity_score: 0,
    pothole_count: 0,
    crack_count: 0,
  });
  const [reports, setReports] = useState(fallbackReports);
  const [healthIndex, setHealthIndex] = useState([]);
  const [priorityScores, setPriorityScores] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  async function loadData() {
    setLoading(true);
    setError("");
    try {
      const [statsData, reportsData, healthData, priorityData] = await Promise.all([
        fetchStats(), 
        fetchReports(),
        fetchHealthIndex().catch(() => []),
        fetchPriorityScores().catch(() => [])
      ]);
      setStats(statsData);
      setReports(reportsData.length ? reportsData : fallbackReports);
      setHealthIndex(healthData);
      setPriorityScores(priorityData);
    } catch (err) {
      setError("Backend belum tersedia penuh, menampilkan data yang bisa dimuat.");
      setReports(fallbackReports);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadData();
  }, []);

  const districtChart = useMemo(() => {
    const map = reports.reduce((acc, report) => {
      acc[report.district] = (acc[report.district] || 0) + 1;
      return acc;
    }, {});
    return Object.entries(map)
      .map(([label, value]) => ({ label, value }))
      .sort((a, b) => b.value - a.value)
      .slice(0, 5);
  }, [reports]);

  const statusChart = useMemo(() => {
    const map = reports.reduce((acc, report) => {
      acc[report.status] = (acc[report.status] || 0) + 1;
      return acc;
    }, {});
    return ["Pending", "Verified", "In Progress", "Resolved"].map((label) => ({
      label,
      value: map[label] || 0,
    }));
  }, [reports]);

  const highSeverity = reports.filter((report) => Number(report.severity_score || 0) >= 80).length;
  const fallbackDetected = reports.filter((report) => report.damage_type && report.damage_type !== "Unknown").length;
  const fallbackAvgSeverity =
    reports.length > 0
      ? Math.round(reports.reduce((total, report) => total + Number(report.severity_score || 0), 0) / reports.length)
      : 0;
  const fallbackPotholes = reports.filter((report) => report.damage_type === "D40_Pothole").length;
  const fallbackCracks = reports.filter((report) => report.damage_type?.includes("Crack")).length;

  return (
    <section className="mx-auto max-w-7xl px-4 py-8 sm:px-6 lg:px-8">
      <div className="mb-6 flex flex-col gap-4 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <p className="text-sm font-semibold uppercase tracking-wide text-civic-600">Dashboard Admin</p>
          <h1 className="mt-2 text-3xl font-bold text-civic-900">Monitoring Laporan Jalan Rusak</h1>
          <p className="mt-2 text-slate-600">Panel operasional untuk verifikasi laporan, prioritas severity, dan pemantauan wilayah.</p>
        </div>
        <button className="btn-secondary" onClick={loadData} disabled={loading}>
          <RefreshCw size={18} className={loading ? "animate-spin" : ""} />
          Refresh
        </button>
      </div>

      {error && <div className="mb-5 rounded-md border border-amber-200 bg-amber-50 p-4 text-sm font-medium text-amber-800">{error}</div>}

      <div className="grid gap-4 md:grid-cols-4">
        <StatCard icon={Activity} label="Total Laporan" value={stats.total_reports || reports.length} />
        <StatCard icon={CalendarDays} label="Laporan Hari Ini" value={stats.today_reports || reports.length} tone="green" />
        <StatCard icon={MapPinned} label="Kecamatan Terbanyak" value={stats.top_district || "-"} tone="amber" />
        <StatCard icon={AlertTriangle} label="Severity Tinggi" value={highSeverity} />
      </div>

      <div className="mt-4 grid gap-4 md:grid-cols-4">
        <StatCard icon={BrainCircuit} label="Total Kerusakan Terdeteksi" value={stats.total_detected_damage || fallbackDetected} />
        <StatCard icon={Gauge} label="Average Severity Score" value={stats.average_severity_score || fallbackAvgSeverity} tone="green" />
        <StatCard icon={Route} label="Jumlah Pothole" value={stats.pothole_count || fallbackPotholes} tone="amber" />
        <StatCard icon={AlertTriangle} label="Jumlah Crack" value={stats.crack_count || fallbackCracks} />
      </div>

      <div className="mt-6 grid gap-6 lg:grid-cols-2">
        <BarChart title="Top Kecamatan" data={districtChart} />
        <BarChart title="Status Laporan" data={statusChart} />
      </div>

      <div className="mt-6">
        <ReportsMapView reports={reports} />
      </div>

      <div className="panel mt-6 overflow-hidden">
        <div className="border-b border-slate-200 px-5 py-4">
          <h2 className="text-lg font-bold text-slate-900">Tabel Laporan</h2>
        </div>
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-slate-200 text-left text-sm">
            <thead className="bg-slate-50 text-xs uppercase tracking-wide text-slate-500">
              <tr>
                <th className="px-5 py-3">ID Laporan</th>
                <th className="px-5 py-3">Tanggal</th>
                <th className="px-5 py-3">Nama Jalan</th>
                <th className="px-5 py-3">Kecamatan</th>
                <th className="px-5 py-3">Damage Type</th>
                <th className="px-5 py-3">Status</th>
                <th className="px-5 py-3">Severity Score</th>
                <th className="px-5 py-3">Confidence</th>
                <th className="px-5 py-3">Foto</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100 bg-white">
              {reports.map((report) => (
                <tr key={report.report_id} className="hover:bg-civic-50/40">
                  <td className="px-5 py-4 font-bold text-civic-700">{report.report_id}</td>
                  <td className="px-5 py-4 text-slate-600">{new Date(report.report_date).toLocaleDateString("id-ID")}</td>
                  <td className="px-5 py-4">
                    <p className="font-semibold text-slate-900">{report.road_name}</p>
                    <p className="text-xs text-slate-500">{new Date(report.report_date).toLocaleString("id-ID")}</p>
                  </td>
                  <td className="px-5 py-4 text-slate-700">{report.district}</td>
                  <td className="px-5 py-4 font-semibold text-slate-800">{report.damage_type || "-"}</td>
                  <td className="px-5 py-4">
                    <span className={`inline-flex rounded-md border px-2.5 py-1 text-xs font-bold ${statusStyle[report.status] || statusStyle.Pending}`}>
                      {report.status}
                    </span>
                  </td>
                  <td className="px-5 py-4">
                    <div className="flex items-center gap-3">
                      <div className="h-2 w-24 overflow-hidden rounded-full bg-slate-100">
                        <div className="h-full rounded-full bg-civic-600" style={{ width: `${Number(report.severity_score || 0)}%` }} />
                      </div>
                      <span className="font-bold text-slate-900">{report.severity_score ?? "-"}</span>
                    </div>
                  </td>
                  <td className="px-5 py-4 font-bold text-slate-900">
                    {formatConfidence(report.confidence)}
                  </td>
                  <td className="px-5 py-4">
                    {firstImage(report.image_path) ? (
                      <img src={firstImage(report.image_path)} alt={report.road_name} className="h-12 w-16 rounded-md object-cover" />
                    ) : (
                      <div className="grid h-12 w-16 place-items-center rounded-md bg-slate-100 text-xs text-slate-400">No foto</div>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* BIG DATA LAKEHOUSE SECTION */}
      <div className="mt-8 border-t-2 border-dashed border-slate-200 pt-8">
        <div className="mb-6 flex items-center gap-3">
          <div className="rounded-md bg-indigo-100 p-2 text-indigo-700">
            <Server size={24} />
          </div>
          <div>
            <h2 className="text-2xl font-bold text-slate-900">Lakehouse Analytics</h2>
            <p className="text-sm text-slate-500">Data real-time yang dihasilkan dari Apache Spark Pipeline (Big Data)</p>
          </div>
        </div>

        <div className="grid gap-6 lg:grid-cols-2">
          {/* Health Index */}
          <div className="panel p-5">
            <h3 className="mb-4 text-lg font-bold text-slate-900">Road Health Index per Kecamatan</h3>
            {healthIndex.length === 0 ? (
              <p className="text-sm text-slate-500">Belum ada data dari Spark Job.</p>
            ) : (
              <div className="space-y-4">
                {healthIndex.map((hi) => (
                  <div key={hi.id}>
                    <div className="mb-1 flex items-center justify-between text-sm">
                      <span className="font-semibold text-slate-700">{hi.district}</span>
                      <span className="font-bold text-slate-900">{hi.road_health_index.toFixed(2)} / 100</span>
                    </div>
                    <div className="h-3 overflow-hidden rounded-full bg-slate-100">
                      <div className="h-full rounded-full bg-emerald-500" style={{ width: `${hi.road_health_index}%` }} />
                    </div>
                    <div className="mt-1 flex gap-4 text-xs text-slate-500">
                      <span>{hi.report_count} laporan</span>
                      <span>{hi.pothole_count} Potholes</span>
                      <span>{hi.crack_count} Cracks</span>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Priority Score Table */}
          <div className="panel overflow-hidden">
            <div className="border-b border-slate-200 px-5 py-4">
              <h3 className="text-lg font-bold text-slate-900">Top 10 Prioritas Perbaikan</h3>
            </div>
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-slate-200 text-left text-sm">
                <thead className="bg-slate-50 text-xs uppercase tracking-wide text-slate-500">
                  <tr>
                    <th className="px-4 py-3">Jalan</th>
                    <th className="px-4 py-3">Kecamatan</th>
                    <th className="px-4 py-3">Kerusakan</th>
                    <th className="px-4 py-3">Priority Score</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100 bg-white">
                  {priorityScores.length === 0 ? (
                    <tr>
                      <td colSpan="4" className="px-4 py-4 text-center text-slate-500">Belum ada data prioritas dari Spark Job.</td>
                    </tr>
                  ) : (
                    priorityScores.map((ps) => (
                      <tr key={ps.id} className="hover:bg-amber-50">
                        <td className="px-4 py-3 font-semibold text-slate-900">{ps.road_name || "Unknown"}</td>
                        <td className="px-4 py-3 text-slate-600">{ps.district || "Unknown"}</td>
                        <td className="px-4 py-3 text-slate-700">{ps.damage_type || "-"}</td>
                        <td className="px-4 py-3">
                          <span className="inline-flex rounded-md bg-amber-100 px-2.5 py-1 font-bold text-amber-800">
                            {ps.priority_score.toFixed(2)}
                          </span>
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
