import { GitPullRequestIcon } from "@phosphor-icons/react";
import { isDismissedReport } from "@posthog/core/inbox/reportMembership";
import { humanizeReportTitle } from "@posthog/core/inbox/reportPresentation";
import type { SignalReport } from "@posthog/shared/types";
import { ReportRowHoverCard } from "@posthog/ui/features/canvas/components/ChannelItemHoverCard";
import {
  ReportRowContextMenu,
  type ReportRowMenuProps,
} from "@posthog/ui/features/canvas/components/ReportRowMenu";
import { ReportRestoreButton } from "@posthog/ui/features/inbox/components/ReportRestoreButton";
import { ReportStateMonogram } from "@posthog/ui/features/inbox/components/ReportStateMonogram";
import { useInboxReportDismissAction } from "@posthog/ui/features/inbox/hooks/useInboxReportDismissAction";
import { SidebarItem } from "@posthog/ui/features/sidebar/components/SidebarItem";
import { useMemo } from "react";

/**
 * One report in a space's Reports tab: a single truncated line, like a
 * session row. The full title, the tl;dr, and the actions live on the shared
 * hover card; the same actions sit on right-click. The row at rest is just
 * the title.
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
  const { dialog, openDialog } = useInboxReportDismissAction(report);
  const title = humanizeReportTitle(report.title, "Untitled report");
  const archived = isDismissedReport(report);

  const menu = useMemo<ReportRowMenuProps>(
    () => ({
      report,
      onOpen: () => onOpen(report.id),
      // Archived rows restore inline instead of archiving again.
      onArchive: archived ? undefined : openDialog,
    }),
    [report, onOpen, archived, openDialog],
  );

  return (
    <>
      <ReportRowContextMenu menu={menu}>
        <ReportRowHoverCard payload={{ report, menu }}>
          <SidebarItem
            depth={0}
            icon={<ReportStateMonogram report={report} />}
            label={<span className="truncate">{title}</span>}
            isActive={isActive}
            onClick={() => onOpen(report.id)}
            endContent={
              <span className="flex items-center gap-1">
                {report.implementation_pr_url && (
                  <GitPullRequestIcon
                    size={13}
                    className="text-(--gray-9)"
                    aria-label="Has a pull request"
                  />
                )}
                {archived && <ReportRestoreButton report={report} />}
              </span>
            }
          />
        </ReportRowHoverCard>
      </ReportRowContextMenu>
      {dialog}
    </>
  );
}
