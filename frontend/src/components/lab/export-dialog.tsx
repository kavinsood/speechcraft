"use client";

import { Button } from "@midday/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@midday/ui/dialog";
import { Label } from "@midday/ui/label";
import { RadioGroup, RadioGroupItem } from "@midday/ui/radio-group";
import { Separator } from "@midday/ui/separator";
import { Spinner } from "@midday/ui/spinner";
import { Switch } from "@midday/ui/switch";
import { useToast } from "@midday/ui/use-toast";
import { useState } from "react";

type ExportDialogProps = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
};

const FORMATS = [
  {
    value: "ljspeech",
    label: "LJSpeech",
    hint: "metadata.csv + wavs/ — the common single-speaker TTS layout.",
  },
  {
    value: "jsonl",
    label: "JSONL manifest",
    hint: "One JSON line per clip (audio path, text, speaker, duration).",
  },
  {
    value: "pairs",
    label: "WAV + txt pairs",
    hint: "A .wav and matching .txt transcript per accepted clip.",
  },
] as const;

export function ExportDialog({ open, onOpenChange }: ExportDialogProps) {
  const { toast, update } = useToast();
  const [format, setFormat] = useState<string>("ljspeech");
  const [includeRejected, setIncludeRejected] = useState(false);
  const [normalizeLoudness, setNormalizeLoudness] = useState(true);
  const [isExporting, setIsExporting] = useState(false);

  const startExport = () => {
    setIsExporting(true);
    onOpenChange(false);
    const { id } = toast({
      title: "Exporting dataset",
      description: `Compiling accepted clips → ${format}.`,
      variant: "progress",
      progress: 0,
      duration: Number.POSITIVE_INFINITY,
    });
    const started = Date.now();
    const interval = setInterval(() => {
      const pct = Math.min(100, Math.round(((Date.now() - started) / 4000) * 100));
      update(id, { id, progress: pct });
      if (pct >= 100) {
        clearInterval(interval);
        setIsExporting(false);
        update(id, {
          id,
          title: "Export complete",
          description: "Your dataset is ready in the project's exports folder.",
          variant: "success",
          duration: 3000,
        });
      }
    }, 120);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-[460px]">
        <div className="p-4">
          <DialogHeader className="mb-6">
            <DialogTitle className="font-serif text-lg">Export dataset</DialogTitle>
            <DialogDescription>
              Compiles your accepted clips and their final transcripts into a
              training-ready dataset.
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-4">
            <div className="space-y-3">
              <Label className="text-xs font-normal uppercase tracking-wide text-[#878787]">
                Format
              </Label>
              <RadioGroup value={format} onValueChange={setFormat} className="space-y-1">
                {FORMATS.map((f) => (
                  <label
                    key={f.value}
                    htmlFor={`fmt-${f.value}`}
                    className="flex cursor-pointer items-start gap-3 py-1.5"
                  >
                    <RadioGroupItem id={`fmt-${f.value}`} value={f.value} className="mt-0.5" />
                    <div className="space-y-0.5">
                      <span className="block text-sm">{f.label}</span>
                      <span className="block text-xs text-[#878787]">{f.hint}</span>
                    </div>
                  </label>
                ))}
              </RadioGroup>
            </div>

            <Separator />

            <div className="space-y-4">
              <div className="flex items-center justify-between">
                <div className="space-y-0.5">
                  <p className="text-sm">Normalize loudness</p>
                  <p className="text-xs text-[#878787]">Even out clip levels on export.</p>
                </div>
                <Switch checked={normalizeLoudness} onCheckedChange={setNormalizeLoudness} />
              </div>
              <div className="flex items-center justify-between">
                <div className="space-y-0.5">
                  <p className="text-sm">Include rejected clips</p>
                  <p className="text-xs text-[#878787]">
                    Off by default — only accepted clips ship.
                  </p>
                </div>
                <Switch checked={includeRejected} onCheckedChange={setIncludeRejected} />
              </div>
            </div>

            <Separator />

            <div className="flex justify-end gap-2">
              <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>
                Cancel
              </Button>
              <Button type="button" onClick={startExport} disabled={isExporting}>
                {isExporting ? (
                  <div className="flex items-center space-x-2">
                    <Spinner className="size-4" />
                    <span>Exporting…</span>
                  </div>
                ) : (
                  <span>Export</span>
                )}
              </Button>
            </div>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}
