import "@/components/ui/globals.css";
import { cn } from "@midday/ui/cn";
import { Toaster } from "@midday/ui/toaster";
import type { Metadata } from "next";
import { Hedvig_Letters_Sans, Hedvig_Letters_Serif } from "next/font/google";
import type { ReactNode } from "react";
import { Providers } from "./providers";

export const metadata: Metadata = {
  title: "Speechcraft",
  description: "Turn raw speech recordings into training-ready voice datasets.",
};

const hedvigSans = Hedvig_Letters_Sans({
  weight: "400",
  subsets: ["latin"],
  display: "swap",
  variable: "--font-hedvig-sans",
});

const hedvigSerif = Hedvig_Letters_Serif({
  weight: "400",
  subsets: ["latin"],
  display: "swap",
  variable: "--font-hedvig-serif",
});

export const viewport = {
  width: "device-width",
  initialScale: 1,
  themeColor: [
    { media: "(prefers-color-scheme: light)" },
    { media: "(prefers-color-scheme: dark)" },
  ],
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body
        className={cn(
          `${hedvigSans.variable} ${hedvigSerif.variable} font-sans`,
          "whitespace-pre-line overscroll-none antialiased",
        )}
      >
        <Providers>
          {children}
          <Toaster />
        </Providers>
      </body>
    </html>
  );
}
