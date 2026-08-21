import { GitPullRequestIcon } from "@phosphor-icons/react";
import { cn } from "@posthog/quill";
import { isDismissedReport } from "@posthog/core/inbox/reportMembership";
import { humanizeReportTitle } from "@posthog/core/inbox/reportPresentation";
import type { SignalReport } from "@posthog/shared/types";
import { ReportRestoreButton } from "@posthog/ui/features/inbox/components/ReportRestoreButton";
import { ReportStateMonogram } from "@posthog/ui/features/inbox/components/ReportStateMonogram";
import { useInboxReportDismissAction } from "@posthog/ui/features/inbox/hooks/useInboxReportDismissAction";

/**
 * One report in a space's Reports tab. A purpose-built row rather than
 * SidebarItem: titles wrap to two lines (the working set bought the vertical
 * room, and truncated imperatives are indistinguishable from each other),
 * which SidebarItem's single-line label slot can't hold. Actions reveal on
 * hover so the resting row is just the title.
 */
export function ReportRow({
  report,
  isActive,
  onOpen,
}: {
  report: SignalReport;
  isActive: boolean;
  onOpen: (reportId: string) => void;
}) {
  const { actionButton, dialog } = useInboxReportDismissAction(report);
  const title = humanizeReportTitle(report.title, "Untitled report");

  return (
    <>
      <div
        role="button"
        tabIndex={0}
        onClick={() => onOpen(report.id)}
        onKeyDown={(event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            onOpen(report.id);
          }
        }}
        data-active={isActive || undefined}
        className={cn(
          "group flex w-full cursor-pointer items-start gap-2 rounded-(--radius-2) px-2 py-1.5 text-left transition-colors",
          isActive ? "bg-fill-selected" : "hover:bg-(--gray-2)",
        )}
      >
        <span className="mt-px shrink-0">
          <ReportStateMonogram report={report} />
        </span>
        <span
          className={cn(
            "line-clamp-2 min-w-0 flex-1 whitespace-normal break-words text-[13px] leading-snug",
            isActive ? "text-gray-12" : "text-gray-11 group-hover:text-gray-12",
          )}
        >
          {title}
        </span>
        <span className="flex shrink-0 items-center gap-1">
          {report.implementation_pr_url && (
            <GitPullRequestIcon
              size={13}
              className="mt-0.5 text-(--gray-9)"
              aria-label="Has a pull request"
            />
          )}
          {/* Propagation guard: acting on a row must not also open it. */}
          {/* biome-ignore lint/a11y/noStaticElementInteractions: guard for the buttons inside, not interactive itself */}
          <span
            onClick={(event) => event.stopPropagation()}
            onKeyDown={(event) => event.stopPropagation()}
            className="opacity-0 transition-opacity focus-within:opacity-100 group-hover:opacity-100"
          >
            {isDismissedReport(report) ? (
              <ReportRestoreButton report={report} />
            ) : (
              actionButton
            )}
          </span>
        </span>
      </div>
      {dialog}
    </>
  );
}
