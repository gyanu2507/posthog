import {
  ArchiveIcon,
  CaretLeftIcon,
  CaretRightIcon,
  ClockIcon,
  FileTextIcon,
  XIcon,
} from "@phosphor-icons/react";
import { humanizeReportTitle } from "@posthog/core/inbox/reportPresentation";
import {
  Button,
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@posthog/quill";
import { isDismissalReasonSnooze } from "@posthog/shared/dismissalReasons";
import type { SignalReport } from "@posthog/shared/types";
import { DetailSection } from "@posthog/ui/features/inbox/components/DetailSection";
import {
  DismissReportDialog,
  type DismissReportDialogResult,
} from "@posthog/ui/features/inbox/components/DismissReportDialog";
import { ReportVerdictBanner } from "@posthog/ui/features/inbox/components/ReportVerdictBanner";
import { SignalReportPriorityBadge } from "@posthog/ui/features/inbox/components/utils/SignalReportPriorityBadge";
import { SignalReportSummaryMarkdown } from "@posthog/ui/features/inbox/components/utils/SignalReportSummaryMarkdown";
import { useInboxBulkActions } from "@posthog/ui/features/inbox/hooks/useInboxBulkActions";
import { RelativeTimestamp } from "@posthog/ui/primitives/RelativeTimestamp";
import { navigateToInboxReportDetail } from "@posthog/ui/router/navigationBridge";
import { useCallback, useEffect, useState } from "react";

/** A keyboard hint chip; quill has no kbd primitive, so plain HTML carries it. */
export function KeyCap({ children }: { children: string }) {
  return (
    <kbd className="rounded border border-(--gray-6) bg-(--gray-2) px-1 font-mono text-[10.5px] text-gray-11">
      {children}
    </kbd>
  );
}

function isTypingTarget(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false;
  return (
    target.isContentEditable ||
    target.tagName === "INPUT" ||
    target.tagName === "TEXTAREA" ||
    target.tagName === "SELECT"
  );
}

/**
 * One report at a time, keyboard-driven: the fast way through a pile of
 * decisions. Walks the needs-a-decision queue in the list's order — j/k (or
 * arrows) move, e opens archive, enter opens the full report, esc leaves.
 * Archiving auto-advances: the archived report drops out of the queue and the
 * next one takes its place under the same index.
 */
export function ReportTriageFocus({
  reports,
  allReports,
  onExit,
}: {
  /** The decision queue, in the list's current sort order. */
  reports: SignalReport[];
  /** Superset backing archive eligibility (mirrors the list shells). */
  allReports: SignalReport[];
  onExit: () => void;
}) {
  const [index, setIndex] = useState(0);
  const [dismissOpen, setDismissOpen] = useState(false);

  // The queue shrinks under us when a report is archived; clamping (rather
  // than resetting) is what makes archive-and-advance work.
  const clamped = Math.min(index, Math.max(0, reports.length - 1));
  const report = reports[clamped];

  const bulkActions = useInboxBulkActions(
    allReports,
    report?.id ?? null,
    "list_row",
  );
  const dismissPending = bulkActions.isSuppressing || bulkActions.isSnoozing;

  const handleDismissConfirm = useCallback(
    async (result: DismissReportDialogResult) => {
      const ok = isDismissalReasonSnooze(result.reason)
        ? await bulkActions.snoozeSelected()
        : await bulkActions.suppressSelected(result);
      if (ok) setDismissOpen(false);
    },
    [bulkActions],
  );

  // Defer = snooze without a dialog: one keystroke, the report re-promotes
  // itself when enough new evidence lands. Auto-advances like archive.
  const deferReport = useCallback(async () => {
    if (bulkActions.snoozeDisabledReason !== null) return;
    await bulkActions.snoozeSelected();
  }, [bulkActions]);

  const goNext = useCallback(
    () => setIndex((i) => Math.min(i + 1, reports.length - 1)),
    [reports.length],
  );
  const goPrev = useCallback(() => setIndex((i) => Math.max(i - 1, 0)), []);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      // The dialog owns the keyboard while open; typing surfaces always do.
      if (dismissOpen || isTypingTarget(event.target)) return;
      if (event.metaKey || event.ctrlKey || event.altKey) return;
      switch (event.key) {
        case "j":
        case "ArrowDown":
        case "ArrowRight":
          event.preventDefault();
          goNext();
          break;
        case "k":
        case "ArrowUp":
        case "ArrowLeft":
          event.preventDefault();
          goPrev();
          break;
        case "e":
          event.preventDefault();
          if (report) setDismissOpen(true);
          break;
        case "d":
          event.preventDefault();
          if (report && !dismissPending) void deferReport();
          break;
        case "Enter":
          event.preventDefault();
          if (report) {
            onExit();
            navigateToInboxReportDetail(report.id);
          }
          break;
        case "Escape":
        case "f":
          event.preventDefault();
          onExit();
          break;
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [
    dismissOpen,
    report,
    dismissPending,
    deferReport,
    goNext,
    goPrev,
    onExit,
  ]);

  if (!report) {
    // The queue ran dry mid-session — every decision is made.
    return (
      <div className="flex flex-col items-center gap-3 py-16">
        <span className="font-medium text-[14px] text-gray-12">
          All decisions made
        </span>
        <span className="text-[13px] text-gray-11">
          Nothing left in the queue.
        </span>
        <Button type="button" variant="outline" size="sm" onClick={onExit}>
          Back to the list
        </Button>
      </div>
    );
  }

  return (
    <>
      <div className="mx-auto flex w-full max-w-3xl flex-col gap-4 px-6 py-4">
        <div className="flex items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            <Button
              type="button"
              variant="outline"
              size="icon-sm"
              aria-label="Previous report"
              disabled={clamped === 0}
              onClick={goPrev}
            >
              <CaretLeftIcon size={14} />
            </Button>
            <Button
              type="button"
              variant="outline"
              size="icon-sm"
              aria-label="Next report"
              disabled={clamped >= reports.length - 1}
              onClick={goNext}
            >
              <CaretRightIcon size={14} />
            </Button>
            <span className="text-[12px] text-gray-10 tabular-nums">
              {clamped + 1} of {reports.length}
            </span>
          </div>
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={onExit}
            className="gap-1"
          >
            <XIcon size={12} />
            Exit focus
          </Button>
        </div>

        <div className="flex flex-col gap-3 rounded-lg border border-border bg-(--color-panel-solid) p-5">
          <div className="flex flex-col gap-1.5">
            <h2 className="font-semibold text-[16px] text-gray-12 leading-snug">
              {humanizeReportTitle(report.title, "Untitled report")}
            </h2>
            <div className="flex items-center gap-2 text-[12px] text-gray-10">
              <SignalReportPriorityBadge priority={report.priority} />
              <span className="tabular-nums">
                {report.signal_count} signal
                {report.signal_count === 1 ? "" : "s"}
              </span>
              <RelativeTimestamp
                timestamp={report.updated_at ?? report.created_at}
                className="text-[12px]"
              />
            </div>
          </div>

          {/* The banner is the accept affordance: Create PR / Continue PR. */}
          <ReportVerdictBanner report={report} />

          <div className="flex items-center gap-2">
            <Tooltip>
              <TooltipTrigger
                render={
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    className="gap-1"
                    disabled={
                      dismissPending ||
                      bulkActions.snoozeDisabledReason !== null
                    }
                    onClick={() => void deferReport()}
                  >
                    <ClockIcon size={12} />
                    Defer
                  </Button>
                }
              />
              <TooltipContent side="bottom">
                {bulkActions.snoozeDisabledReason ??
                  "Snooze until enough new evidence arrives"}
              </TooltipContent>
            </Tooltip>
            <Button
              type="button"
              variant="outline"
              size="sm"
              className="gap-1"
              disabled={dismissPending}
              onClick={() => setDismissOpen(true)}
            >
              <ArchiveIcon size={12} />
              Dismiss…
            </Button>
          </div>

          {/* The proof stays folded: triage reads the verdict, research opens
              the full report. Unfolding here is the middle ground. */}
          <DetailSection
            Icon={FileTextIcon}
            title="How we know"
            collapsible
            defaultCollapsed
          >
            <SignalReportSummaryMarkdown
              content={report.summary}
              fallback="No summary yet. The agent is still investigating."
              variant="detail"
              pending={report.status === "in_progress"}
            />
          </DetailSection>
        </div>

        <div className="flex items-center justify-center gap-4 text-[12px] text-gray-10">
          <span className="flex items-center gap-1">
            <KeyCap>j</KeyCap>
            <KeyCap>k</KeyCap> move
          </span>
          <span className="flex items-center gap-1">
            <KeyCap>d</KeyCap> defer
          </span>
          <span className="flex items-center gap-1">
            <KeyCap>e</KeyCap> dismiss
          </span>
          <span className="flex items-center gap-1">
            <KeyCap>↵</KeyCap> open
          </span>
          <span className="flex items-center gap-1">
            <KeyCap>esc</KeyCap> exit
          </span>
        </div>
      </div>

      {dismissOpen && (
        <DismissReportDialog
          open
          onOpenChange={setDismissOpen}
          report={report}
          isSubmitting={dismissPending}
          snoozeDisabledReason={bulkActions.snoozeDisabledReason}
          onConfirm={handleDismissConfirm}
        />
      )}
    </>
  );
}
