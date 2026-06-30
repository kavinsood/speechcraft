import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import TagComposer, { parseStatusLabel } from "./TagComposer";

afterEach(() => {
  cleanup();
});

describe("TagComposer", () => {
  it("routes reserved status words to review status instead of reviewer tags", async () => {
    const onReviewStatusChange = vi.fn().mockResolvedValue(undefined);
    const onAddReviewerTag = vi.fn().mockResolvedValue(undefined);

    render(
      <TagComposer
        reviewStatus="unresolved"
        machineFindings={[]}
        reviewerTags={[]}
        suggestions={[]}
        onReviewStatusChange={onReviewStatusChange}
        onAddReviewerTag={onAddReviewerTag}
        onRemoveReviewerTag={vi.fn()}
      />,
    );

    const input = screen.getByPlaceholderText("Add tag (press Enter)");
    fireEvent.change(input, { target: { value: "rejected" } });
    fireEvent.keyDown(input, { key: "Enter" });

    await waitFor(() => {
      expect(onReviewStatusChange).toHaveBeenCalledWith("rejected");
    });
    expect(onAddReviewerTag).not.toHaveBeenCalled();
  });

  it("adds a reviewer tag on Enter and clears the input after success", async () => {
    const onAddReviewerTag = vi.fn().mockResolvedValue(undefined);

    render(
      <TagComposer
        reviewStatus="unresolved"
        machineFindings={[]}
        reviewerTags={[]}
        suggestions={[]}
        onReviewStatusChange={vi.fn()}
        onAddReviewerTag={onAddReviewerTag}
        onRemoveReviewerTag={vi.fn()}
      />,
    );

    const input = screen.getByPlaceholderText("Add tag (press Enter)") as HTMLInputElement;
    fireEvent.change(input, { target: { value: "good energy" } });
    fireEvent.keyDown(input, { key: "Enter" });

    await waitFor(() => {
      expect(onAddReviewerTag).toHaveBeenCalledWith("good energy");
    });
    expect(input.value).toBe("");
  });

  it("does not add tags when comma is pressed", async () => {
    const onAddReviewerTag = vi.fn().mockResolvedValue(undefined);

    render(
      <TagComposer
        reviewStatus="unresolved"
        machineFindings={[]}
        reviewerTags={[]}
        suggestions={[]}
        onReviewStatusChange={vi.fn()}
        onAddReviewerTag={onAddReviewerTag}
        onRemoveReviewerTag={vi.fn()}
      />,
    );

    const input = screen.getByPlaceholderText("Add tag (press Enter)");
    fireEvent.change(input, { target: { value: "good energy" } });
    fireEvent.keyDown(input, { key: "," });

    expect(onAddReviewerTag).not.toHaveBeenCalled();
  });

  it("blocks duplicate machine finding labels", async () => {
    const onAddReviewerTag = vi.fn().mockResolvedValue(undefined);

    render(
      <TagComposer
        reviewStatus="unresolved"
        machineFindings={[{ code: "low_energy", label: "low energy" }]}
        reviewerTags={[]}
        suggestions={[]}
        onReviewStatusChange={vi.fn()}
        onAddReviewerTag={onAddReviewerTag}
        onRemoveReviewerTag={vi.fn()}
      />,
    );

    const input = screen.getByPlaceholderText("Add tag (press Enter)");
    fireEvent.change(input, { target: { value: "Low Energy" } });
    fireEvent.keyDown(input, { key: "Enter" });

    expect(onAddReviewerTag).not.toHaveBeenCalled();
    expect(screen.getByText("That label already exists as a pipeline finding.")).toBeTruthy();
  });

  it("does not add a duplicate custom tag", async () => {
    const onAddReviewerTag = vi.fn().mockResolvedValue(undefined);

    render(
      <TagComposer
        reviewStatus="unresolved"
        machineFindings={[]}
        reviewerTags={["Good Energy"]}
        suggestions={[]}
        onReviewStatusChange={vi.fn()}
        onAddReviewerTag={onAddReviewerTag}
        onRemoveReviewerTag={vi.fn()}
      />,
    );

    const input = screen.getByPlaceholderText("Add tag (press Enter)") as HTMLInputElement;
    fireEvent.change(input, { target: { value: "good energy" } });
    fireEvent.keyDown(input, { key: "Enter" });

    expect(onAddReviewerTag).not.toHaveBeenCalled();
    expect(input.value).toBe("");
  });

  it("shows suggestions only while focused and filtered by input", () => {
    render(
      <TagComposer
        reviewStatus="unresolved"
        machineFindings={[]}
        reviewerTags={[]}
        suggestions={["Good Energy", "Mouth Noise"]}
        onReviewStatusChange={vi.fn()}
        onAddReviewerTag={vi.fn()}
        onRemoveReviewerTag={vi.fn()}
      />,
    );

    expect(screen.queryByText("Good Energy")).toBeNull();

    const input = screen.getByPlaceholderText("Add tag (press Enter)");
    fireEvent.focus(input);
    fireEvent.change(input, { target: { value: "mouth" } });

    expect(screen.getByText("Mouth Noise")).toBeTruthy();
    expect(screen.queryByText("Good Energy")).toBeNull();
  });

  it("preserves typed input and shows an error when save fails", async () => {
    const onAddReviewerTag = vi.fn().mockRejectedValue(new Error("network failure"));

    render(
      <TagComposer
        reviewStatus="unresolved"
        machineFindings={[]}
        reviewerTags={[]}
        suggestions={[]}
        onReviewStatusChange={vi.fn()}
        onAddReviewerTag={onAddReviewerTag}
        onRemoveReviewerTag={vi.fn()}
      />,
    );

    const input = screen.getByPlaceholderText("Add tag (press Enter)") as HTMLInputElement;
    fireEvent.change(input, { target: { value: "mouth noise" } });
    fireEvent.keyDown(input, { key: "Enter" });

    await waitFor(() => {
      expect(screen.getByText("network failure")).toBeTruthy();
    });
    expect(input.value).toBe("mouth noise");
  });

  it("does not render pipeline findings or review status pills", () => {
    render(
      <TagComposer
        reviewStatus="accepted"
        machineFindings={[{ code: "contains_oov", label: "contains OOV" }]}
        reviewerTags={["good energy"]}
        suggestions={[]}
        onReviewStatusChange={vi.fn()}
        onAddReviewerTag={vi.fn()}
        onRemoveReviewerTag={vi.fn()}
      />,
    );

    expect(screen.queryByText("contains OOV")).toBeNull();
    expect(screen.queryByRole("button", { name: /accepted/i })).toBeNull();
    expect(screen.getByRole("button", { name: /good energy/i })).toBeTruthy();
  });

  it("removes reviewer tags through the callback", async () => {
    const onRemoveReviewerTag = vi.fn().mockResolvedValue(undefined);

    render(
      <TagComposer
        reviewStatus="unresolved"
        machineFindings={[]}
        reviewerTags={["mouth noise"]}
        suggestions={[]}
        onReviewStatusChange={vi.fn()}
        onAddReviewerTag={vi.fn()}
        onRemoveReviewerTag={onRemoveReviewerTag}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: /mouth noise/i }));

    await waitFor(() => {
      expect(onRemoveReviewerTag).toHaveBeenCalledWith("mouth noise");
    });
  });

  it("parses reserved status labels case-insensitively", () => {
    expect(parseStatusLabel("Accepted")).toBe("accepted");
    expect(parseStatusLabel("good energy")).toBeNull();
  });
});
