"use client";

import { Card } from "@midday/ui/card";
import { cn } from "@midday/ui/cn";
import { Icons } from "@midday/ui/icons";
import { Input } from "@midday/ui/input";
import { Textarea } from "@midday/ui/textarea";
import { useEffect, useRef, useState } from "react";
import { SavingBar } from "@/components/saving-bar";
import { type LabClip, formatQcScore } from "./lab-data";

type TranscriptPanelProps = {
  clip: LabClip;
  allTags: string[];
  onTranscriptChange: (text: string) => void;
  onTagsChange: (tags: string[]) => void;
};

export function TranscriptPanel({
  clip,
  allTags,
  onTranscriptChange,
  onTagsChange,
}: TranscriptPanelProps) {
  const [draft, setDraft] = useState(clip.transcript);
  const [isPending, setIsPending] = useState(false);
  const [tagInput, setTagInput] = useState("");
  const textareaRef = useRef<HTMLTextAreaElement | null>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Reset the draft when the active clip changes.
  useEffect(() => {
    setDraft(clip.transcript);
    setIsPending(false);
    if (debounceRef.current) clearTimeout(debounceRef.current);
  }, [clip.id, clip.transcript]);

  // Auto-grow the textarea to fit content.
  useEffect(() => {
    const el = textareaRef.current;
    if (!el) return;
    el.style.height = "auto";
    el.style.height = `${el.scrollHeight}px`;
  }, [draft]);

  const handleTranscript = (value: string) => {
    setDraft(value);
    setIsPending(true);
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => {
      onTranscriptChange(value);
      setIsPending(false);
    }, 600);
  };

  const addTag = (raw: string) => {
    const tag = raw.trim();
    if (!tag) return;
    if (clip.tags.some((t) => t.toLowerCase() === tag.toLowerCase())) return;
    onTagsChange([...clip.tags, tag]);
    setTagInput("");
  };

  const removeTag = (tag: string) => {
    onTagsChange(clip.tags.filter((t) => t !== tag));
  };

  const suggestions = allTags
    .filter((tag) => !clip.tags.some((t) => t.toLowerCase() === tag.toLowerCase()))
    .slice(0, 8);

  return (
    <section className="flex flex-col border border-border">
      <div className="flex h-11 items-center border-b border-border px-4">
        <span className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
          Transcript
        </span>
      </div>

      <div className="relative flex flex-col gap-4 p-4">
        <Textarea
          ref={textareaRef}
          value={draft}
          onChange={(event) => handleTranscript(event.target.value)}
          rows={1}
          placeholder="Transcript text"
          className="resize-none border-border text-sm leading-relaxed"
        />

        <p className="text-xs text-muted-foreground">
          Source: <span className="text-foreground">{clip.variant === "source" ? "aligned" : clip.variant}</span>
          <span className="mx-2 text-border">•</span>
          Edits autosave — use Undo to revert.
        </p>

        {/* QC score cards + tag composer */}
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
          <Card className="border-border p-4">
            <p className="mb-3 text-xs font-medium uppercase tracking-wide text-muted-foreground">
              QC scores
            </p>
            <div className="grid grid-cols-2 gap-4">
              <div>
                <p className="text-[11px] uppercase tracking-wide text-muted-foreground">
                  Transcript confidence
                </p>
                <p className="mt-1 text-2xl font-medium tabular-nums">
                  {formatQcScore(clip.transcriptConfidence)}
                </p>
              </div>
              <div>
                <p className="text-[11px] uppercase tracking-wide text-muted-foreground">
                  Speaker purity
                </p>
                <p className="mt-1 text-2xl font-medium tabular-nums">
                  {formatQcScore(clip.speakerPurity)}
                </p>
              </div>
            </div>
          </Card>

          <Card className="border-border p-4">
            <p className="mb-3 text-xs font-medium uppercase tracking-wide text-muted-foreground">
              Custom tags
            </p>

            <div className="flex flex-wrap gap-1.5">
              {clip.tags.length > 0 ? (
                clip.tags.map((tag) => (
                  <button
                    key={tag}
                    type="button"
                    onClick={() => removeTag(tag)}
                    className="inline-flex items-center gap-1 bg-secondary px-2 py-1 text-xs text-secondary-foreground hover:bg-secondary/70"
                    title="Remove tag"
                  >
                    {tag}
                    <Icons.Close className="size-3" />
                  </button>
                ))
              ) : (
                <span className="text-xs text-muted-foreground">No custom tags yet.</span>
              )}
            </div>

            <div className="mt-3">
              <Input
                value={tagInput}
                onChange={(event) => setTagInput(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === "Enter" || event.key === ",") {
                    event.preventDefault();
                    addTag(tagInput);
                  }
                }}
                placeholder="Add tag (press Enter)"
                className="h-8 text-xs"
              />
            </div>

            {suggestions.length > 0 ? (
              <div className="mt-2 flex flex-wrap gap-1.5">
                {suggestions.map((tag) => (
                  <button
                    key={tag}
                    type="button"
                    onClick={() => addTag(tag)}
                    className={cn(
                      "border border-border px-2 py-0.5 text-[11px] text-muted-foreground",
                      "hover:text-foreground",
                    )}
                  >
                    + {tag}
                  </button>
                ))}
              </div>
            ) : null}
          </Card>
        </div>

        <SavingBar isPending={isPending} />
      </div>
    </section>
  );
}
