export default function StatCard({ icon: Icon, label, value, tone = "blue" }) {
  const tones = {
    blue: "bg-civic-50 text-civic-700",
    green: "bg-emerald-50 text-emerald-700",
    amber: "bg-amber-50 text-amber-700",
  };

  return (
    <div className="panel p-5">
      <div className="flex items-center justify-between gap-4">
        <div>
          <p className="text-sm font-medium text-slate-500">{label}</p>
          <p className="mt-2 text-3xl font-bold text-slate-900">{value}</p>
        </div>
        {Icon && (
          <div className={`grid h-12 w-12 place-items-center rounded-md ${tones[tone]}`}>
            <Icon size={24} />
          </div>
        )}
      </div>
    </div>
  );
}
