import { useState } from 'react';

interface SubMenuProps {
  title: string;
  options: Array<{ id: string; title: string; description: string }>;
  queueStats?: {
    active: number;
    unmatched: number;
    trueFail: number;
    posted: number;
  };
  showQueueStats?: boolean;
  onSelect: (id: string) => void;
  onBack: () => void;
}

export function SubMenu({ title, options, queueStats, showQueueStats, onSelect, onBack }: SubMenuProps) {
  const [input, setInput] = useState('');

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (input.trim()) {
      onSelect(input.trim());
      setInput('');
    }
  };

  return (
    <div>
      <div className="border-t border-[#00ff00] mb-4"></div>
      <div className="mb-2 text-[#00d4ff]">  {title}</div>
      <div className="border-t border-[#00ff00] mb-6"></div>

      {showQueueStats && queueStats && (
        <div className="mb-6 text-[#888888]">
          <span>  Queue snapshot: </span>
          <span className="text-[#00ff00]">active={queueStats.active}</span>
          <span>  </span>
          <span className="text-[#00ff00]">unmatched={queueStats.unmatched}</span>
          <span>  </span>
          <span className="text-[#00ff00]">true_fail={queueStats.trueFail}</span>
          <span>  </span>
          <span className="text-[#00ff00]">posted={queueStats.posted}</span>
        </div>
      )}

      <div className="space-y-2 mb-6">
        {options.map((option) => (
          <div key={option.id} className="group">
            <span className="text-[#ffff00]">  [{option.id}]</span>
            <span className="text-[#00ff00] ml-2">{option.title}</span>
            <span className="text-[#00d4ff] ml-2">—</span>
            <span className="text-[#888888] ml-2">{option.description}</span>
          </div>
        ))}
        <div className="group">
          <span className="text-[#ffff00]">  [b]</span>
          <span className="text-[#00ff00] ml-2">Back</span>
          <span className="text-[#00d4ff] ml-2">—</span>
          <span className="text-[#888888] ml-2">Return to the main category menu</span>
        </div>
      </div>

      <form onSubmit={handleSubmit} className="flex items-center gap-2">
        <span className="text-[#00d4ff]">  &gt;</span>
        <input
          type="text"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          className="bg-transparent border-none outline-none text-[#00ff00] w-16"
          autoFocus
        />
      </form>
    </div>
  );
}
