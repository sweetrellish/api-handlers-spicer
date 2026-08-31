import { Play, AlertCircle } from 'lucide-react';
import { Category } from '../data/menuData';

interface CategoryViewProps {
  category: Category;
  queueStats: any;
  onActionSelect: (categoryId: string, actionId: string) => void;
  onBack: () => void;
}

export function CategoryView({ category, queueStats, onActionSelect, onBack }: CategoryViewProps) {
  const showQueueSnapshot = category.id === '2';

  return (
    <div className="space-y-8">
      <div>
        <h2 className="text-3xl font-bold text-[#00d4ff] mb-2">{category.title}</h2>
        <p className="text-gray-400">{category.description}</p>
      </div>

      {showQueueSnapshot && (
        <div className="bg-gradient-to-r from-cyan-950/30 to-transparent border border-cyan-500/20 rounded-xl p-6">
          <div className="flex items-center gap-3 mb-3">
            <AlertCircle className="w-5 h-5 text-cyan-400" />
            <h3 className="text-lg font-semibold text-cyan-300">Queue Snapshot</h3>
          </div>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div>
              <span className="text-gray-400 text-sm">Active</span>
              <p className="text-2xl font-bold text-cyan-400">{queueStats.active}</p>
            </div>
            <div>
              <span className="text-gray-400 text-sm">Unmatched</span>
              <p className="text-2xl font-bold text-purple-400">{queueStats.unmatched}</p>
            </div>
            <div>
              <span className="text-gray-400 text-sm">True Fail</span>
              <p className="text-2xl font-bold text-red-400">{queueStats.trueFail}</p>
            </div>
            <div>
              <span className="text-gray-400 text-sm">Posted</span>
              <p className="text-2xl font-bold text-green-400">{queueStats.posted}</p>
            </div>
          </div>
        </div>
      )}

      <div className="grid grid-cols-1 gap-4">
        {category.actions.map((action) => (
          <button
            key={action.id}
            onClick={() => onActionSelect(category.id, action.id)}
            className="group relative bg-gradient-to-r from-black/60 to-black/40 border border-[#00ff00]/10 rounded-lg p-5 text-left transition-all duration-200 hover:border-[#00ff00]/40 hover:shadow-md hover:shadow-[#00ff00]/10"
          >
            <div className="absolute inset-0 bg-gradient-to-r from-[#00ff00]/5 to-transparent opacity-0 group-hover:opacity-100 transition-opacity rounded-lg" />

            <div className="relative flex items-start gap-4">
              <div className="flex-shrink-0 w-10 h-10 rounded-lg bg-[#00ff00]/10 flex items-center justify-center group-hover:bg-[#00ff00]/20 transition-colors">
                <Play className="w-5 h-5 text-[#00ff00]" />
              </div>

              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-3 mb-1">
                  <span className="text-xs font-mono text-gray-600">[{action.id}]</span>
                  <h3 className="text-lg font-semibold text-white group-hover:text-[#00d4ff] transition-colors">
                    {action.title}
                  </h3>
                </div>
                <p className="text-sm text-gray-400 leading-relaxed">
                  {action.description}
                </p>
              </div>

              <div className="flex-shrink-0 opacity-0 group-hover:opacity-100 transition-opacity">
                <div className="w-8 h-8 rounded-full bg-[#00ff00]/20 flex items-center justify-center">
                  <span className="text-[#00ff00] text-xl">→</span>
                </div>
              </div>
            </div>
          </button>
        ))}
      </div>

      <div className="pt-6 border-t border-gray-800">
        <button
          onClick={onBack}
          className="px-6 py-3 bg-gray-800/50 hover:bg-gray-700/50 border border-gray-700 rounded-lg text-gray-300 hover:text-white transition-all duration-200"
        >
          ← Back to Main Menu
        </button>
      </div>
    </div>
  );
}
