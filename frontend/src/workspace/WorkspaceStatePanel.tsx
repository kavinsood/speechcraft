type WorkspaceStatePanelProps = {
  title: string;
  message: string;
  actionLabel?: string;
  onAction?: () => void;
};

export default function WorkspaceStatePanel({
  title,
  message,
  actionLabel,
  onAction,
}: WorkspaceStatePanelProps) {
  return (
    <div className="workspace-state-panel">
      <p className="eyebrow">Workspace State</p>
      <h2>{title}</h2>
      <p>{message}</p>
      {actionLabel && onAction ? (
        <button className="primary-button" type="button" onClick={onAction}>
          {actionLabel}
        </button>
      ) : null}
    </div>
  );
}
