import { REPORTS_INBOX_STATUS_FILTER } from "@posthog/core/inbox/reportFiltering";
import { reportNeedsDecision } from "@posthog/core/inbox/reportInboxSections";
import { useInboxAllReports } from "@posthog/ui/features/inbox/hooks/useInboxAllReports";
import { useMemo } from "react";

/**
 * The number the inbox badges show: loaded reports waiting on a decision,
 * under the current reviewer scope and the inbox's own filters.
 *
 * Deliberately the SAME query the inbox page renders from — same server
 * params, same cache key, same pages — so the badge and the page's "Needs a
 * decision" count are two reads of one dataset and cannot disagree. The badge
 * moving with the page's filters is the price of that guarantee, and the
 * cheaper one: a badge that contradicts the list it points at is worse than a
 * badge that follows it.
 */
export function useInboxDecisionCount(): number {
  const { scopedReports } = useInboxAllReports({
    refetchIntervalMs: 60_000,
    statusFilter: REPORTS_INBOX_STATUS_FILTER,
  });
  return useMemo(
    () => scopedReports.filter(reportNeedsDecision).length,
    [scopedReports],
  );
}
