import { Icons } from "@midday/ui/icons";
import Link from "next/link";
import { LoginVideoBackground } from "@/components/login-video-background";

// Shared shell for the login/onboarding-teaser chain (/login through
// /login6). Lives in a layout (not each page) so the video background +
// testimonials persist across navigation between these pages instead of
// unmounting and remounting (which restarted the video and reset the
// testimonial rotation on every "Continue" click).
// Chain steps read ?project=/&run= from the URL (useSearchParams) — render dynamically.
export const dynamic = "force-dynamic";

export default function LoginChainLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <div className="min-h-screen bg-background flex relative">
      {/* Logo - Fixed position matching website header exactly */}
      <div className="fixed top-0 left-0 right-0 z-50 w-full">
        <nav className="w-full pointer-events-none">
          <div className="relative py-3 xl:py-4 px-4 sm:px-4 md:px-4 lg:px-4 xl:px-6 2xl:px-8 flex items-center">
            <Link
              href="https://midday.ai"
              className="flex items-center gap-2 hover:opacity-80 active:opacity-80 transition-opacity duration-200 pointer-events-auto"
            >
              <div className="w-6 h-6">
                <Icons.LogoSmall className="w-full h-full text-foreground lg:text-white" />
              </div>
            </Link>
          </div>
        </nav>
      </div>

      {/* Left Side - Video Background (persists across pages in this chain) */}
      <LoginVideoBackground />

      {/* Right Side - Per-page content */}
      <div className="w-full lg:w-1/2 flex flex-col justify-center items-center p-8 lg:p-12 pb-2">
        <div className="w-full max-w-md flex flex-col h-full">
          <div className="flex-1 flex flex-col justify-center">{children}</div>
        </div>
      </div>
    </div>
  );
}
