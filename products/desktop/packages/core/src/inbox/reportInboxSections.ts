import type { SignalReport, SignalReportPriority } from "@posthog/shared/types";

/**
 * The global reports inbox shows every live report in two sections: what needs
 * a decision from a person, and what is being watched (the agent still
 * working, or findings filed for awareness). Resolved and archived reports
 * come from a separate fetch and sit behind their own collapsed section.
 */
export type InboxReportSort = "evidence" | "priority" | "newest";

export interface InboxReportSections {
  /** Waiting on a person: ready to act, a PR to review, stuck, or failed. */
  decision: SignalReport[];
  /** Being watched: runs in flight, and findings with nothing to decide. */
  monitoring: SignalReport[];
}

/**
 * Whether a report is asking for a decision. A PR outranks everything (review
 * is a decision even while the run is still moving); a ready report only asks
 * when it is actionable and not already handled elsewhere — otherwise it is
 * an observation, which is monitoring.
 */
export function reportNeedsDecision(report: SignalReport): boolean {
  // Only an open PR is review work; a merged one is history, and the report
  // classifies by its own state (it outlived its fix if it's still ready).
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

const PRIORITY_RANK: Record<SignalReportPriority, number> = {
  P0: 0,
  P1: 1,
  P2: 2,
  P3: 3,
  P4: 4,
};

function priorityRank(report: SignalReport): number {
  return report.priority
    ? PRIORITY_RANK[report.priority]
    : Number.MAX_SAFE_INTEGER;
}

function timestampMs(report: SignalReport): number {
  const value = report.updated_at ?? report.created_at;
  if (!value) return 0;
  const ms = new Date(value).getTime();
  return Number.isFinite(ms) ? ms : 0;
}

/**
 * Order a section for the chosen sort. "Evidence" is the honest stand-in for
 * impact until reports carry a real affected-user count: how much backing a
 * finding has (signal count, then accumulated weight). Every sort breaks
 * remaining ties by newest activity.
 */
export function sortInboxReports(
  reports: SignalReport[],
  sort: InboxReportSort,
): SignalReport[] {
  const byRecency = (a: SignalReport, b: SignalReport) =>
    timestampMs(b) - timestampMs(a);
  const sorted = [...reports];
  switch (sort) {
    case "evidence":
      sorted.sort(
        (a, b) =>
          b.signal_count - a.signal_count ||
          b.total_weight - a.total_weight ||
          byRecency(a, b),
      );
      break;
    case "priority":
      sorted.sort(
        (a, b) => priorityRank(a) - priorityRank(b) || byRecency(a, b),
      );
      break;
    case "newest":
      sorted.sort(byRecency);
      break;
  }
  return sorted;
}

export function buildInboxReportSections(
  reports: SignalReport[],
  sort: InboxReportSort,
): InboxReportSections {
  const decision: SignalReport[] = [];
  const monitoring: SignalReport[] = [];
  for (const report of reports) {
    (reportNeedsDecision(report) ? decision : monitoring).push(report);
  }
  return {
    decision: sortInboxReports(decision, sort),
    monitoring: sortInboxReports(monitoring, sort),
  };
}
