import { useMemo, useRef, useState, type ChangeEvent } from "react";
import { ApiError, createImportBatch, deleteProject, uploadProjectSourceRecording } from "../api";
import type { Project } from "../types";
import WorkspaceStatePanel from "../workspace/WorkspaceStatePanel";

type IngestPageProps = {
  activeProject: Project | null;
  projectLoadStatus: "loading" | "ready" | "error";
  projectLoadError: string | null;
  onRetryProjects: () => void;
  onImportComplete: (projectId: string) => void;
};

type UploadStatus = "queued" | "uploading" | "completed" | "failed";

type UploadItem = {
  key: string;
  name: string;
  size: number;
  status: UploadStatus;
  progress: number;
  error: string | null;
};

const MAX_VISIBLE_FILES = 200;
const allowedWavTypes = new Set([
  "",
  "application/octet-stream",
  "audio/wav",
  "audio/x-wav",
  "audio/wave",
  "audio/vnd.wave",
]);

function getFileDisplayName(file: File): string {
  return file.webkitRelativePath || file.name;
}

function getFileKey(file: File): string {
  return `${getFileDisplayName(file)}:${file.size}:${file.lastModified}`;
}

function isWavFile(file: File): boolean {
  if (!file.name.toLowerCase().endsWith(".wav")) {
    return false;
  }
  return !file.type || allowedWavTypes.has(file.type);
}

function formatFileSize(size: number): string {
  if (size < 1024) {
    return `${size} B`;
  }
  if (size < 1024 * 1024) {
    return `${(size / 1024).toFixed(1)} KB`;
  }
  if (size < 1024 * 1024 * 1024) {
    return `${(size / (1024 * 1024)).toFixed(1)} MB`;
  }
  return `${(size / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

function makeProjectId(name: string): string {
  const slug =
    name
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "")
      .slice(0, 42) || "project";
  return `${slug}-${Date.now().toString(36)}`;
}

function getErrorMessage(error: unknown, fallback: string): string {
  if (error instanceof ApiError && error.message.trim()) {
    return error.message;
  }
  if (error instanceof Error && error.message.trim()) {
    return error.message;
  }
  return fallback;
}

async function runSerialUploadQueue(
  files: File[],
  uploadOne: (file: File, index: number) => Promise<void>,
): Promise<void> {
  for (let index = 0; index < files.length; index += 1) {
    await uploadOne(files[index], index);
  }
}

export default function IngestPage({
  projectLoadStatus,
  projectLoadError,
  onRetryProjects,
  onImportComplete,
}: IngestPageProps) {
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const folderInputRef = useRef<HTMLInputElement | null>(null);
  const [projectName, setProjectName] = useState("");
  const [selectedFiles, setSelectedFiles] = useState<File[]>([]);
  const [uploadItems, setUploadItems] = useState<UploadItem[]>([]);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [fileNotice, setFileNotice] = useState<string | null>(null);

  const trimmedProjectName = projectName.trim();
  const totalBytes = useMemo(
    () => selectedFiles.reduce((total, file) => total + file.size, 0),
    [selectedFiles],
  );
  const completedUploads = uploadItems.filter((item) => item.status === "completed").length;
  const canSubmit = trimmedProjectName.length > 0 && selectedFiles.length > 0 && !isSubmitting;
  const visibleFiles = selectedFiles.slice(0, MAX_VISIBLE_FILES);

  function addFiles(files: File[]) {
    if (files.length === 0) {
      return;
    }
    const validFiles = files.filter(isWavFile);
    const rejectedCount = files.length - validFiles.length;
    setSelectedFiles((current) => {
      const seen = new Set(current.map(getFileKey));
      const next = [...current];
      let duplicateCount = 0;
      for (const file of validFiles) {
        const key = getFileKey(file);
        if (seen.has(key)) {
          duplicateCount += 1;
          continue;
        }
        seen.add(key);
        next.push(file);
      }
      const notices = [];
      if (rejectedCount > 0) {
        notices.push(`${rejectedCount} non-WAV file${rejectedCount === 1 ? "" : "s"} skipped`);
      }
      if (duplicateCount > 0) {
        notices.push(`${duplicateCount} duplicate file${duplicateCount === 1 ? "" : "s"} skipped`);
      }
      setFileNotice(notices.length > 0 ? `${notices.join(". ")}.` : null);
      return next;
    });
  }

  function handleFileSelection(event: ChangeEvent<HTMLInputElement>) {
    addFiles(Array.from(event.currentTarget.files ?? []));
    event.currentTarget.value = "";
  }

  function removeFile(fileKey: string) {
    setSelectedFiles((current) => current.filter((file) => getFileKey(file) !== fileKey));
    setUploadItems([]);
    setSubmitError(null);
  }

  async function handleSubmit() {
    const name = trimmedProjectName;
    if (!name) {
      setSubmitError("Project name is required.");
      return;
    }
    if (selectedFiles.length === 0) {
      setSubmitError("Select at least one WAV file before importing.");
      return;
    }
    if (isSubmitting) {
      return;
    }

    const projectId = makeProjectId(name);
    setIsSubmitting(true);
    setSubmitError(null);
    setFileNotice(null);
    setUploadItems(
      selectedFiles.map((file) => ({
        key: getFileKey(file),
        name: getFileDisplayName(file),
        size: file.size,
        status: "queued",
        progress: 0,
        error: null,
      })),
    );

    try {
      await createImportBatch({ id: projectId, name });
      await runSerialUploadQueue(selectedFiles, async (file, index) => {
        setUploadItems((items) =>
          items.map((item, itemIndex) =>
            itemIndex === index ? { ...item, status: "uploading", progress: 0, error: null } : item,
          ),
        );
        await uploadProjectSourceRecording(projectId, file, (progress) => {
          setUploadItems((items) =>
            items.map((item, itemIndex) => (itemIndex === index ? { ...item, progress } : item)),
          );
        });
        setUploadItems((items) =>
          items.map((item, itemIndex) =>
            itemIndex === index ? { ...item, status: "completed", progress: 100 } : item,
          ),
        );
      });
      onImportComplete(projectId);
    } catch (error) {
      const message = getErrorMessage(error, "Import failed. The selected files are still staged.");
      setSubmitError(message);
      setUploadItems((items) =>
        items.map((item) =>
          item.status === "uploading" || item.status === "queued"
            ? { ...item, status: item.status === "uploading" ? "failed" : item.status, error: message }
            : item,
        ),
      );
      try {
        await deleteProject(projectId);
      } catch {
        setSubmitError(`${message} The partially created project could not be cleaned up automatically.`);
      }
    } finally {
      setIsSubmitting(false);
    }
  }

  return (
    <section className="step-page ingest-page">
      {projectLoadStatus === "error" ? (
        <WorkspaceStatePanel
          title="Projects unavailable"
          message={projectLoadError ?? "The project list could not be loaded."}
          actionLabel="Retry project load"
          onAction={onRetryProjects}
        />
      ) : (
        <div className="ingest-shell">
          <main className="panel ingest-create-panel">
            <div className="panel-header">
              <div>
                <p className="eyebrow">Project</p>
                <h3>Create project and import</h3>
              </div>
            </div>

            <label className="ingest-field" htmlFor="project-name">
              <span>Project name</span>
              <input
                id="project-name"
                type="text"
                value={projectName}
                onChange={(event) => setProjectName(event.target.value)}
                placeholder="Project name"
                disabled={isSubmitting}
              />
            </label>

            <section className="ingest-source-actions" aria-label="Source files">
              <input
                ref={fileInputRef}
                type="file"
                multiple
                accept=".wav,audio/wav,audio/x-wav"
                onChange={handleFileSelection}
                hidden
              />
              <input
                ref={(node) => {
                  folderInputRef.current = node;
                  if (node) {
                    node.setAttribute("webkitdirectory", "");
                    node.setAttribute("directory", "");
                  }
                }}
                type="file"
                multiple
                accept=".wav,audio/wav,audio/x-wav"
                onChange={handleFileSelection}
                hidden
              />
              <button
                className="primary-button"
                type="button"
                onClick={() => fileInputRef.current?.click()}
                disabled={isSubmitting}
              >
                Select WAV files
              </button>
              <button
                className="ghost-button"
                type="button"
                onClick={() => folderInputRef.current?.click()}
                disabled={isSubmitting}
              >
                Select folder (optional)
              </button>
              <span>
                Only WAV files are supported right now. Folder selection is best-effort when the browser supports it.
              </span>
            </section>

            {fileNotice ? <p className="ingest-inline-message">{fileNotice}</p> : null}
            {submitError ? <p className="ingest-inline-message ingest-inline-error">{submitError}</p> : null}

            <section className="ingest-file-list" aria-label="Selected files">
              <div className="ingest-file-list-header">
                <div>
                  <p className="eyebrow">Selected files</p>
                  <h4>{selectedFiles.length === 0 ? "No files selected" : `${selectedFiles.length} staged`}</h4>
                </div>
                <div className="ingest-file-actions">
                  <span>{formatFileSize(totalBytes)}</span>
                  <button
                    className="ghost-button"
                    type="button"
                    onClick={() => {
                      setSelectedFiles([]);
                      setUploadItems([]);
                      setFileNotice(null);
                      setSubmitError(null);
                    }}
                    disabled={selectedFiles.length === 0 || isSubmitting}
                  >
                    Clear all
                  </button>
                </div>
              </div>

              {selectedFiles.length === 0 ? (
                <p className="ingest-empty-copy">No files selected</p>
              ) : (
                <ul>
                  {visibleFiles.map((file) => {
                    const key = getFileKey(file);
                    return (
                      <li key={key}>
                        <div>
                          <strong>{getFileDisplayName(file)}</strong>
                          <span>{formatFileSize(file.size)}</span>
                        </div>
                        <button
                          className="ghost-button"
                          type="button"
                          onClick={() => removeFile(key)}
                          disabled={isSubmitting}
                        >
                          Remove
                        </button>
                      </li>
                    );
                  })}
                </ul>
              )}
              {selectedFiles.length > MAX_VISIBLE_FILES ? (
                <p className="ingest-empty-copy">
                  Showing first {MAX_VISIBLE_FILES} files. All {selectedFiles.length} files will be imported.
                </p>
              ) : null}
            </section>

            {uploadItems.length > 0 ? (
              <section className="ingest-upload-status" aria-label="Upload progress">
                <div className="ingest-file-list-header">
                  <div>
                    <p className="eyebrow">Upload</p>
                    <h4>
                      {completedUploads} of {uploadItems.length} files completed
                    </h4>
                  </div>
                </div>
                <ul>
                  {uploadItems.slice(0, MAX_VISIBLE_FILES).map((item) => (
                    <li key={item.key}>
                      <div className="ingest-upload-row">
                        <strong>{item.name}</strong>
                        <span>{item.status}</span>
                      </div>
                      <div className="ingest-progress-track" aria-hidden="true">
                        <span style={{ width: `${item.progress}%` }} />
                      </div>
                      {item.error ? <p className="ingest-inline-error">{item.error}</p> : null}
                    </li>
                  ))}
                </ul>
              </section>
            ) : null}

            <div className="ingest-submit-row">
              <button className="primary-button" type="button" onClick={handleSubmit} disabled={!canSubmit}>
                {isSubmitting ? "Importing..." : "Create project and import"}
              </button>
            </div>
          </main>
        </div>
      )}
    </section>
  );
}
