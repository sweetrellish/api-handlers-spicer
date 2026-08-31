import { Database, Webhook, BarChart3, HardDrive, Settings, LogOut } from 'lucide-react';
import { Category } from '../data/menuData';

interface MainDashboardProps {
  categories: Category[];
  queueStats: any;
  onCategorySelect: (categoryId: string) => void;
}

const iconMap: Record<string, any> = {
  Database,
  Webhook,
  BarChart3,
  HardDrive,
  Settings
};

export function MainDashboard({ categories, queueStats, onCategorySelect }: MainDashboardProps) {
  return (
    <div className="space-y-8">
      <div>
        <h2 className="text-3xl font-bold text-[#00d4ff] mb-2">Functional Categories</h2>
        <p className="text-gray-400">Select a category to access admin operations</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {categories.map((category) => {
          const Icon = iconMap[category.icon] || Database;
          return (
            <button
              key={category.id}
              onClick={() => onCategorySelect(category.id)}
              className="group relative bg-gradient-to-br from-black/60 to-black/40 border border-[#00ff00]/20 rounded-xl p-6 text-left transition-all duration-300 hover:border-[#00ff00]/50 hover:shadow-lg hover:shadow-[#00ff00]/20 hover:-translate-y-1"
            >
              <div className="absolute inset-0 bg-gradient-to-br from-[#00ff00]/5 to-transparent opacity-0 group-hover:opacity-100 transition-opacity rounded-xl" />

              <div className="relative">
                <div className="flex items-start justify-between mb-4">
                  <div className="p-3 bg-[#00ff00]/10 rounded-lg group-hover:bg-[#00ff00]/20 transition-colors">
                    <Icon className="w-6 h-6 text-[#00ff00]" />
                  </div>
                  <span className="text-xs font-mono text-gray-500">#{category.id}</span>
                </div>

                <h3 className="text-xl font-bold text-white mb-2 group-hover:text-[#00d4ff] transition-colors">
                  {category.title}
                </h3>
                <p className="text-sm text-gray-400 leading-relaxed">
                  {category.description}
                </p>

                <div className="mt-4 pt-4 border-t border-gray-800">
                  <span className="text-xs text-[#00ff00]/60 group-hover:text-[#00ff00] transition-colors">
                    {category.actions.length} operations available
                  </span>
                </div>
              </div>
            </button>
          );
        })}

        {/* Quit button */}
        <button
          onClick={() => onCategorySelect('quit')}
          className="group relative bg-gradient-to-br from-red-950/40 to-black/40 border border-red-500/20 rounded-xl p-6 text-left transition-all duration-300 hover:border-red-500/50 hover:shadow-lg hover:shadow-red-500/20"
        >
          <div className="absolute inset-0 bg-gradient-to-br from-red-500/5 to-transparent opacity-0 group-hover:opacity-100 transition-opacity rounded-xl" />

          <div className="relative">
            <div className="flex items-start justify-between mb-4">
              <div className="p-3 bg-red-500/10 rounded-lg group-hover:bg-red-500/20 transition-colors">
                <LogOut className="w-6 h-6 text-red-400" />
              </div>
              <span className="text-xs font-mono text-gray-500">q</span>
            </div>

            <h3 className="text-xl font-bold text-white mb-2 group-hover:text-red-400 transition-colors">
              Quit
            </h3>
            <p className="text-sm text-gray-400 leading-relaxed">
              Exit the admin console
            </p>
          </div>
        </button>
      </div>
    </div>
  );
}
