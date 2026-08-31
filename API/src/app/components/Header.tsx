import { ArrowLeft } from 'lucide-react';
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
              <h1 className="text-2xl font-bold text-[#00d4ff] tracking-tight">
                SpicerAPI
              </h1>
              <p className="text-sm text-gray-500">Admin Console</p>
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
