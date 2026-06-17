import { BrowserRouter, Link, NavLink, Route, Routes } from "react-router-dom";
import { BarChart3, Building2, FilePlus2, MapPinned } from "lucide-react";
import Dashboard from "./pages/Dashboard.jsx";
import Landing from "./pages/Landing.jsx";
import ReportForm from "./pages/ReportForm.jsx";

function Shell() {
  const navClass = ({ isActive }) =>
    `inline-flex items-center gap-2 rounded-md px-3 py-2 text-sm font-semibold transition ${
      isActive ? "bg-civic-600 text-white" : "text-slate-600 hover:bg-civic-50 hover:text-civic-700"
    }`;

  return (
    <div className="min-h-screen bg-[#f6fbff]">
      <header className="sticky top-0 z-40 border-b border-slate-200 bg-white/95 backdrop-blur">
        <div className="mx-auto flex max-w-7xl items-center justify-between px-4 py-3 sm:px-6 lg:px-8">
          <Link to="/" className="flex items-center gap-3">
            <div className="grid h-10 w-10 place-items-center rounded-md bg-civic-600 text-white">
              <MapPinned size={22} />
            </div>
            <div>
              <p className="text-sm font-bold text-civic-900">SRIS Surabaya</p>
              <p className="text-xs text-slate-500">Crowdsourcing Module</p>
            </div>
          </Link>
          <nav className="flex items-center gap-2">
            <NavLink to="/report" className={navClass}>
              <FilePlus2 size={17} />
              <span className="hidden sm:inline">Lapor</span>
            </NavLink>
            <NavLink to="/admin" className={navClass}>
              <BarChart3 size={17} />
              <span className="hidden sm:inline">Admin</span>
            </NavLink>
          </nav>
        </div>
      </header>
      <main>
        <Routes>
          <Route path="/" element={<Landing />} />
          <Route path="/report" element={<ReportForm />} />
          <Route path="/admin" element={<Dashboard />} />
        </Routes>
      </main>
      <footer className="border-t border-slate-200 bg-white">
        <div className="mx-auto flex max-w-7xl flex-col gap-2 px-4 py-5 text-sm text-slate-500 sm:flex-row sm:items-center sm:justify-between sm:px-6 lg:px-8">
          <span>Smart Road Intelligence System Surabaya</span>
          <span className="inline-flex items-center gap-2">
            <Building2 size={16} />
            Pemerintah Kota Surabaya
          </span>
        </div>
      </footer>
    </div>
  );
}

export default function App() {
  return (
    <BrowserRouter>
      <Shell />
    </BrowserRouter>
  );
}
