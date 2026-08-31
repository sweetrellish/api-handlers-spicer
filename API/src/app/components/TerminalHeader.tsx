interface TerminalHeaderProps {
  queuePending: number;
  queuePosted: number;
}

export function TerminalHeader({ queuePending, queuePosted }: TerminalHeaderProps) {
  return (
    <div className="mb-8">
      <pre className="text-[#00ff00] leading-tight mb-4 text-sm whitespace-pre">
{`  █████████             ███                                █████████   ███████████  █████
 ███░░░░░███           ░░░                                ███░░░░░███ ░░███░░░░░███░░███
░███    ░░░  ████████  ████   ██████   ██████  ████████  ░███    ░███  ░███    ░███ ░███
░░█████████ ░░███░░███░░███  ███░░███ ███░░███░░███░░███ ░███████████  ░██████████  ░███
 ░░░░░░░░███ ░███ ░███ ░███ ░███ ░░░ ░███████  ░███ ░░░  ░███░░░░░███  ░███░░░░░░   ░███
 ███    ░███ ░███ ░███ ░███ ░███  ███░███░░░   ░███      ░███    ░███  ░███         ░███
░░█████████  ░███████  █████░░██████ ░░██████  █████     █████   █████ █████        █████
 ░░░░░░░░░   ░███░░░  ░░░░░  ░░░░░░   ░░░░░░  ░░░░░     ░░░░░   ░░░░░ ░░░░░        ░░░░░
             ░███
             █████
            ░░░░░`}
      </pre>

      <div className="flex justify-between items-center mb-6">
        <span className="text-[#00d4ff]">Spicer Bros. Admin Console</span>
        <span className="text-[#888888] text-sm">written by Ryan Ellis</span>
      </div>

      <div className="text-[#ffff00] mb-6">
        <span>  Queue → pending: {queuePending}  │  posted: {queuePosted}</span>
      </div>
    </div>
  );
}
