// Presentational keyboard legend, mirroring Midday's shortcut convention
// (small bordered keycap pills, e.g. transaction-shortcuts.tsx). The actual
// hotkeys are wired in LabWorkstation via react-hotkeys-hook.

const SHORTCUTS: { keys: string[]; label: string }[] = [
  { keys: ["Space"], label: "Play / pause" },
  { keys: ["Enter"], label: "Accept & next" },
  { keys: ["⇧", "Enter"], label: "Reject & next" },
  { keys: ["↑", "↓"], label: "Prev / next clip" },
  { keys: ["⌘", "Z"], label: "Undo" },
];

export function KeyboardBar() {
  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-2 border-t border-border px-4 py-2.5">
      {SHORTCUTS.map((shortcut) => (
        <div key={shortcut.label} className="flex items-center gap-1.5">
          <span className="flex items-center gap-0.5">
            {shortcut.keys.map((key) => (
              <kbd
                key={key}
                className="flex h-5 min-w-5 items-center justify-center border border-border px-1.5 text-[10px] text-muted-foreground"
              >
                {key}
              </kbd>
            ))}
          </span>
          <span className="text-[10px] text-muted-foreground">{shortcut.label}</span>
        </div>
      ))}
    </div>
  );
}
