import { reportNeedsDecision } from "@posthog/core/inbox/reportInboxSections";
import { useInboxAllReports } from "@posthog/ui/features/inbox/hooks/useInboxAllReports";
import { useMemo } from "react";

/**
 * The one number the inbox badges mean: reports waiting on a decision under
 * the current reviewer scope. Follows the For you / Entire project toggle and
 * deliberately ignores the inbox page's own search and filter chrome — the
 * badge is global chrome, so transient narrowing must not move it. The page
 * reconciles the two by captioning its section count against this total.
 */
export function useInboxDecisionCount(): number {
  const { scopedReports } = useInboxAllReports({
    ignoreFilters: true,
    refetchIntervalMs: 60_000,
  });
  return useMemo(
    () => scopedReports.filter(reportNeedsDecision).length,
    [scopedReports],
  );
}
