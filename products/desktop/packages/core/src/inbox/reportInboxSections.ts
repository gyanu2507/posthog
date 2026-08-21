import type { SignalReport } from "@posthog/shared/types";

/**
 * The global reports inbox shows every live report in two sections: what needs
 * a decision from a person, and what is being watched (the agent still
 * working, or findings filed for awareness). Resolved and archived reports
 * come from a separate fetch and sit behind their own collapsed section.
 */
export interface InboxReportSections {
  /** Waiting on a person: ready to act, a PR to review, stuck, or failed. */
  decision: SignalReport[];
  /** Being watched: runs in flight, and findings with nothing to decide. */
  monitoring: SignalReport[];
}

/**
 * Whether a report is asking for a decision. An open PR outranks everything
 * (review is a decision even while the run is still moving); a merged one is
 * history, and the report classifies by its own state. A ready report only
 * asks when it is actionable and not already handled elsewhere — otherwise it
 * is an observation, which is monitoring.
 */
export function reportNeedsDecision(report: SignalReport): boolean {
  if (report.implementation_pr_url && !report.implementation_pr_merged) {
    return true;
  }
  if (report.status === "pending_input" || report.status === "failed") {
    return true;
  }
  return (
    report.status === "ready" &&
    !report.already_addressed &&
    report.actionability !== "not_actionable"
  );
}

/**
 * Partition the loaded list into the two sections, preserving its order. The
 * list arrives sorted by the user's own sort (applied server-side by the
 * filter bar), and every number on the page is derived from this one list —
 * a second sort or a second query is how counts start disagreeing.
 */
export function partitionInboxReports(
  reports: SignalReport[],
): InboxReportSections {
  const decision: SignalReport[] = [];
  const monitoring: SignalReport[] = [];
  for (const report of reports) {
    (reportNeedsDecision(report) ? decision : monitoring).push(report);
  }
  return { decision, monitoring };
}
