interface QueueStatsProps {
  stats: {
    pending: number;
    posted: number;
    active: number;
    unmatched: number;
    trueFail: number;
    processing: number;
  };
}

export function QueueStats({ stats }: QueueStatsProps) {
  const statItems = [
    { label: 'Pending', value: stats.pending, color: 'text-yellow-400' },
    { label: 'Posted', value: stats.posted, color: 'text-green-400' },
    { label: 'Active', value: stats.active, color: 'text-cyan-400' },
    { label: 'Unmatched', value: stats.unmatched, color: 'text-purple-400' },
    { label: 'Failed', value: stats.trueFail, color: 'text-red-400' }
  ];

  return (
    <div className="flex items-center gap-6 text-sm">
      <span className="text-gray-500 font-semibold">Queue →</span>
      {statItems.map((item, idx) => (
        <div key={idx} className="flex items-center gap-2">
          <span className="text-gray-400">{item.label}:</span>
          <span className={`font-bold ${item.color}`}>{item.value}</span>
          {idx < statItems.length - 1 && (
            <span className="text-gray-700 ml-2">│</span>
          )}
        </div>
      ))}
    </div>
  );
}
