import type { SignalReport } from "@posthog/shared/types";
import { describe, expect, it } from "vitest";

import { partitionInboxReports } from "./reportInboxSections";

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
    const sections = partitionInboxReports([
      report(overrides as Partial<SignalReport>),
    ]);
    expect(sections.decision.length).toBe(section === "decision" ? 1 : 0);
    expect(sections.monitoring.length).toBe(section === "monitoring" ? 1 : 0);
  });

  it("partition preserves the list's own order within each section", () => {
    const sections = partitionInboxReports([
      report({ id: "d1" }),
      report({ id: "m1", status: "in_progress" }),
      report({ id: "d2" }),
      report({ id: "m2", status: "candidate" }),
    ]);
    expect(sections.decision.map((r) => r.id)).toEqual(["d1", "d2"]);
    expect(sections.monitoring.map((r) => r.id)).toEqual(["m1", "m2"]);
  });
});
