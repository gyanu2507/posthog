import { deriveHeadline } from "@posthog/core/inbox/reportPresentation";
import { humanizeReportTitle } from "@posthog/core/inbox/reportPresentation";
import {
  Item,
  ItemContent,
  ItemDescription,
  ItemGroup,
  ItemSeparator,
  ItemTitle,
} from "@posthog/quill";
import { formatRelativeTimeShort } from "@posthog/shared";
import type { SignalReport } from "@posthog/shared/types";
import {
  type ReportRowMenuProps,
  ReportRowMenuList,
} from "@posthog/ui/features/canvas/components/ReportRowMenu";
import { ReportStateMonogram } from "@posthog/ui/features/inbox/components/ReportStateMonogram";
import { SignalReportPriorityBadge } from "@posthog/ui/features/inbox/components/utils/SignalReportPriorityBadge";
import { useMemo } from "react";

export interface ReportPreviewPayload {
  report: SignalReport;
  menu: ReportRowMenuProps;
}

/**
 * The report's card on the sidebar's shared hover popup: what the truncated
 * row can't say (the full title, the tl;dr, the backing), then the row's
 * actions — the same shape a session's card has, so pointing across the two
 * kinds of row reads as one card changing subject.
 */
export function ReportPreview({
  payload,
  onAction,
}: {
  payload: ReportPreviewPayload;
  onAction: () => void;
}) {
  const { report, menu } = payload;
  const headline = useMemo(
    () => deriveHeadline(report.summary),
    [report.summary],
  );
  const updated = report.updated_at ?? report.created_at;
  return (
    <ItemGroup className="gap-0!">
      <Item size="xs" className="flex-nowrap p-2">
        <ItemContent className="min-w-0">
          <ItemTitle className="flex items-baseline gap-2 break-words">
            <span className="flex size-4 shrink-0 translate-y-0.5 items-center justify-center">
              <ReportStateMonogram report={report} />
            </span>
            <span className="min-w-0">
              {humanizeReportTitle(report.title, "Untitled report")}
            </span>
          </ItemTitle>
          <ItemDescription className="flex items-center gap-1.5">
            <SignalReportPriorityBadge priority={report.priority} />
            <span>
              Report · {report.signal_count} signal
              {report.signal_count === 1 ? "" : "s"}
              {updated ? ` · updated ${formatRelativeTimeShort(updated)}` : ""}
            </span>
          </ItemDescription>
        </ItemContent>
      </Item>
      {headline && (
        <>
          <ItemSeparator className="my-0" />
          <div className="min-w-0 p-2 text-[12px] text-gray-11 leading-snug">
            {headline}
          </div>
        </>
      )}
      <ItemSeparator className="my-0" />
      <div className="p-1">
        <ReportRowMenuList menu={menu} onAction={onAction} />
      </div>
    </ItemGroup>
  );
}
