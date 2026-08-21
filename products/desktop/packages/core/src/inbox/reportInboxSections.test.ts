import type { SignalReport } from "@posthog/shared/types";
import { describe, expect, it } from "vitest";

import {
  buildInboxReportSections,
  sortInboxReports,
} from "./reportInboxSections";

function report(overrides: Partial<SignalReport>): SignalReport {
  return {
    id: overrides.id ?? "r",
    title: "A report",
    summary: null,
    status: "ready",
    total_weight: 1,
    signal_count: 1,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
    artefact_count: 0,
    ...overrides,
  } as SignalReport;
}

describe("reportInboxSections", () => {
  it.each([
    // A ready observation with nothing to decide is monitoring, not a decision.
    [{ status: "ready", actionability: "not_actionable" }, "monitoring"],
    [{ status: "ready", already_addressed: true }, "monitoring"],
    [{ status: "ready" }, "decision"],
    [{ status: "pending_input" }, "decision"],
    [{ status: "failed" }, "decision"],
    // A PR to review is a decision even mid-run or on an FYI report.
    [
      { status: "in_progress", implementation_pr_url: "https://gh/pr/1" },
      "decision",
    ],
    // A merged PR is history: the report classifies by its own state, so a
    // still-ready report reads as a decision again, and a running one as
    // monitoring — never as "review".
    [
      {
        status: "ready",
        implementation_pr_url: "https://gh/pr/9",
        implementation_pr_merged: true,
      },
      "decision",
    ],
    [
      {
        status: "in_progress",
        implementation_pr_url: "https://gh/pr/9",
        implementation_pr_merged: true,
      },
      "monitoring",
    ],
    [
      {
        status: "ready",
        actionability: "not_actionable",
        implementation_pr_url: "https://gh/pr/2",
      },
      "decision",
    ],
    [{ status: "potential" }, "monitoring"],
    [{ status: "candidate" }, "monitoring"],
    [{ status: "in_progress" }, "monitoring"],
  ] as const)("%j lands in %s", (overrides, section) => {
    const sections = buildInboxReportSections(
      [report(overrides as Partial<SignalReport>)],
      "newest",
    );
    expect(sections.decision.length).toBe(section === "decision" ? 1 : 0);
    expect(sections.monitoring.length).toBe(section === "monitoring" ? 1 : 0);
  });

  it("evidence sort puts the most-backed report first, weight breaking count ties", () => {
    const ids = sortInboxReports(
      [
        report({ id: "light", signal_count: 2, total_weight: 2 }),
        report({ id: "heavy-tie", signal_count: 5, total_weight: 9 }),
        report({ id: "light-tie", signal_count: 5, total_weight: 3 }),
      ],
      "evidence",
    ).map((r) => r.id);
    expect(ids).toEqual(["heavy-tie", "light-tie", "light"]);
  });

  it("priority sort ranks P0 first and unprioritized last regardless of recency", () => {
    const ids = sortInboxReports(
      [
        report({ id: "none-new", updated_at: "2026-06-09T00:00:00Z" }),
        report({
          id: "p2",
          priority: "P2",
          updated_at: "2026-06-01T00:00:00Z",
        }),
        report({
          id: "p0",
          priority: "P0",
          updated_at: "2026-05-01T00:00:00Z",
        }),
      ],
      "priority",
    ).map((r) => r.id);
    expect(ids).toEqual(["p0", "p2", "none-new"]);
  });
});
