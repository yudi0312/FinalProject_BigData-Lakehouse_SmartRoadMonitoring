import { useEffect } from "react";
import L from "leaflet";
import { MapContainer, Marker, TileLayer, useMap } from "react-leaflet";

delete L.Icon.Default.prototype._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png",
  iconUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png",
  shadowUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png",
});

const SURABAYA_CENTER = [-7.2575, 112.7521];

function Recenter({ position }) {
  const map = useMap();

  useEffect(() => {
    if (position) {
      map.flyTo(position, 16, { duration: 0.8 });
    }
  }, [map, position]);

  return null;
}

export default function ReportMap({ position, onPositionChange, height = "360px" }) {
  const markerPosition = position || SURABAYA_CENTER;

  return (
    <div className="overflow-hidden rounded-lg border border-slate-200" style={{ height }}>
      <MapContainer center={markerPosition} zoom={position ? 16 : 12} scrollWheelZoom>
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        <Recenter position={position} />
        <Marker
          draggable
          position={markerPosition}
          eventHandlers={{
            dragend: (event) => {
              const next = event.target.getLatLng();
              onPositionChange?.([next.lat, next.lng]);
            },
          }}
        />
      </MapContainer>
    </div>
  );
}
