// Workstation shell. No sidebar and no auth (local single-user tool). The
// logo, project switcher, export, and re-run all live in the Lab top-bar now;
// the content fills the full width. QC and Lab are a feedback loop toggled
// within the content, not peer destinations.
export default function WorkstationLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return <>{children}</>;
}
