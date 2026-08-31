import { useState } from 'react';

interface MainMenuProps {
  options: Array<{ id: string; title: string; description: string }>;
  onSelect: (id: string) => void;
}

export function MainMenu({ options, onSelect }: MainMenuProps) {
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
      <div className="mb-2 text-[#00d4ff]">  Main Menu — Functional Categories</div>
      <div className="border-t border-[#00ff00] mb-6"></div>

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
          <span className="text-[#ffff00]">  [q]</span>
          <span className="text-[#00ff00] ml-2">Quit</span>
          <span className="text-[#00d4ff] ml-2">—</span>
          <span className="text-[#888888] ml-2">Exit the admin console</span>
        </div>
      </div>

      <form onSubmit={handleSubmit} className="flex items-center gap-2">
        <span className="text-[#00d4ff]">  Select:</span>
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
