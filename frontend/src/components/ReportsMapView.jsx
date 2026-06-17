import L from "leaflet";
import { MapContainer, Marker, Popup, TileLayer } from "react-leaflet";

delete L.Icon.Default.prototype._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png",
  iconUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png",
  shadowUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png",
});

const SURABAYA_CENTER = [-7.2575, 112.7521];

function formatConfidence(value) {
  if (value === null || value === undefined || value === "") return "-";
  return `${Math.round(Number(value) * 100)}%`;
}

export default function ReportsMapView({ reports }) {
  const validReports = reports.filter((report) => report.latitude && report.longitude);

  return (
    <div className="panel overflow-hidden">
      <div className="border-b border-slate-200 px-5 py-4">
        <h2 className="text-lg font-bold text-slate-900">Peta Sebaran Laporan</h2>
      </div>
      <div className="h-80">
        <MapContainer center={SURABAYA_CENTER} zoom={12} scrollWheelZoom>
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />
          {validReports.map((report) => (
            <Marker key={report.report_id} position={[report.latitude, report.longitude]}>
              <Popup>
                <strong>{report.road_name}</strong>
                <br />
                Damage Type: {report.damage_type || "-"}
                <br />
                Severity Score: {report.severity_score ?? "-"}
                <br />
                Confidence: {formatConfidence(report.confidence)}
                <br />
                Tanggal Laporan: {new Date(report.report_date).toLocaleString("id-ID")}
                <br />
                Status: {report.status}
              </Popup>
            </Marker>
          ))}
        </MapContainer>
      </div>
    </div>
  );
}
