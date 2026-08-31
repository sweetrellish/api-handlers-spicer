import { ArrowLeft } from 'lucide-react';
import { useEffect, useState } from 'react';
import { QueueStats } from './QueueStats';

interface HeaderProps {
  queueStats: {
    pending: number;
    posted: number;
    active: number;
    unmatched: number;
    trueFail: number;
    processing: number;
  };
  onBack?: () => void;
}

export function Header({ queueStats, onBack }: HeaderProps) {
  const [asciiTitle, setAsciiTitle] = useState('SpicerAPI');
  const basePrefix = typeof window !== 'undefined' && window.location.pathname.startsWith('/ops-gui') ? '/ops-gui/' : '/';

  useEffect(() => {
    let active = true;

    fetch(`${basePrefix}ascii-art/rebel-ascii.txt`)
      .then((response) => (response.ok ? response.text() : 'SpicerAPI'))
      .then((text) => {
        if (!active) {
          return;
        }
        setAsciiTitle(text.trimEnd() || 'SpicerAPI');
      })
      .catch(() => {
        if (active) {
          setAsciiTitle('SpicerAPI');
        }
      });

    return () => {
      active = false;
    };
  }, [basePrefix]);

  return (
    <header className="border-b border-[#00ff00]/20 bg-black/40 backdrop-blur-md sticky top-0 z-50">
      <div className="max-w-7xl mx-auto px-6 py-4">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-4">
            {onBack && (
              <button
                onClick={onBack}
                className="p-2 rounded-lg hover:bg-[#00ff00]/10 transition-colors text-[#00ff00]"
              >
                <ArrowLeft size={20} />
              </button>
            )}
            <div>
              <pre className="text-[8px] sm:text-[9px] leading-tight font-bold text-cyan-300 m-0 whitespace-pre overflow-x-auto">
                {asciiTitle}
              </pre>
              <p className="text-xs uppercase tracking-[0.22em] text-gray-500 mt-1">Admin Console</p>
            </div>
          </div>

          <div className="text-sm text-gray-400">
            written by <span className="text-[#00d4ff]">Ryan Ellis</span>
          </div>
        </div>

        <QueueStats stats={queueStats} />
      </div>
    </header>
  );
}
