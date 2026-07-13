import Link from "next/link";

// Placeholder root — replaced when the wizard landing (/login) is ported in
// task 3. Exists so the scaffold boots and routes are reachable during setup.
export default function Home() {
  return (
    <main className="flex min-h-screen flex-col items-center justify-center gap-4">
      <h1 className="font-serif text-2xl">Speechcraft</h1>
      <p className="text-sm text-[#878787]">Scaffold online. Routes:</p>
      <nav className="flex gap-4 text-sm underline">
        <Link href="/login">/login (wizard)</Link>
        <Link href="/lab">/lab (workstation)</Link>
      </nav>
    </main>
  );
}
