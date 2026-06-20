export const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

export async function fetchStats() {
  const response = await fetch(`${API_BASE_URL}/stats`);
  if (!response.ok) throw new Error("Gagal mengambil statistik.");
  return response.json();
}

export async function fetchReports() {
  const response = await fetch(`${API_BASE_URL}/reports`);
  if (!response.ok) throw new Error("Gagal mengambil laporan.");
  return response.json();
}

export async function submitReport(formData) {
  const response = await fetch(`${API_BASE_URL}/reports`, {
    method: "POST",
    body: formData,
  });
  if (!response.ok) {
    const error = await response.json().catch(() => ({}));
    throw new Error(error.detail || "Gagal mengirim laporan.");
  }
  return response.json();
}

export async function fetchHealthIndex() {
  const response = await fetch(`${API_BASE_URL}/bigdata/health-index`);
  if (!response.ok) throw new Error("Gagal mengambil data Road Health Index.");
  return response.json();
}

export async function fetchPriorityScores() {
  const response = await fetch(`${API_BASE_URL}/bigdata/priority-score`);
  if (!response.ok) throw new Error("Gagal mengambil data Priority Score.");
  return response.json();
}

