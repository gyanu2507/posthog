import {
  CaretDownIcon,
  EnvelopeSimpleIcon,
  GitPullRequestIcon,
} from "@phosphor-icons/react";
import { humanizeIdentifier } from "@posthog/core/inbox/activityLog";
import {
  deriveHeadline,
  humanizeReportTitle,
  parsePrUrl,
} from "@posthog/core/inbox/reportPresentation";
import {
  INBOX_DISMISSED_STATUS_FILTER,
  REPORTS_INBOX_STATUS_FILTER,
} from "@posthog/core/inbox/reportFiltering";
import { partitionInboxReports } from "@posthog/core/inbox/reportInboxSections";
import {
  Button,
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
  Skeleton,
  Spinner,
} from "@posthog/quill";
import type { SignalReport } from "@posthog/shared/types";
import { InboxScopeSelect } from "@posthog/ui/features/inbox/components/InboxScopeSelect";
import { InboxSearchFilterBar } from "@posthog/ui/features/inbox/components/InboxSearchFilterBar";
import { SuggestedReviewerAvatarStack } from "@posthog/ui/features/inbox/components/SuggestedReviewerAvatarStack";
import { useInboxReportDismissAction } from "@posthog/ui/features/inbox/hooks/useInboxReportDismissAction";
import { SignalReportPriorityBadge } from "@posthog/ui/features/inbox/components/utils/SignalReportPriorityBadge";
import { useInboxAllReports } from "@posthog/ui/features/inbox/hooks/useInboxAllReports";
import { useInboxReportsInfinite } from "@posthog/ui/features/inbox/hooks/useInboxReports";
import { RelativeTimestamp } from "@posthog/ui/primitives/RelativeTimestamp";
import {
  navigateToAgents,
  navigateToInboxReportDetail,
} from "@posthog/ui/router/navigationBridge";
import { useEffect, useMemo, useState } from "react";
import { openExternalUrl } from "@posthog/ui/shell/openExternal";

/** Rows shown per section before "Show more" — a scan, not a scroll. */
const SECTION_PREVIEW_LIMIT = 5;

/**
 * How many reports auto-paging will load before stopping and marking counts
 * incomplete ("+"). Bounds the page's cost on enormous projects while keeping
 * counts exact for realistic scoped populations.
 */
const AUTOPAGE_REPORT_LIMIT = 400;

/**
 * The global reports inbox: every report in the project on one page,
 * sectioned by what it asks (a decision, or just watching), quantified by the
 * evidence behind it, and triageable one at a time in focus mode. The
 * per-space sidebar list stays the working set; this is everything else.
 */
export function ReportsInboxView() {
  const {
    scopedReports,
    allReports,
    isLoading,
    hasNextPage,
    isFetchingNextPage,
    fetchNextPage,
  } = useInboxAllReports({ statusFilter: REPORTS_INBOX_STATUS_FILTER });

  const sections = useMemo(
    () => partitionInboxReports(scopedReports),
    [scopedReports],
  );
  const decisionPrCount = useMemo(
    () =>
      sections.decision.filter(
        (r) => r.implementation_pr_url && !r.implementation_pr_merged,
      ).length,
    [sections.decision],
  );

  // Load the whole scoped list rather than counting a window of it: every
  // number on this page (and the nav badge, which reads the same query) is
  // derived from these rows, so an unloaded page is a wrong count. Capped so
  // an enormous project degrades to explicit "+" counts instead of unbounded
  // fetching.
  const countsComplete = !hasNextPage;
  useEffect(() => {
    if (
      !hasNextPage ||
      isFetchingNextPage ||
      isLoading ||
      scopedReports.length >= AUTOPAGE_REPORT_LIMIT
    ) {
      return;
    }
    fetchNextPage();
  }, [
    hasNextPage,
    isFetchingNextPage,
    isLoading,
    scopedReports.length,
    fetchNextPage,
  ]);

  const isEmpty =
    !isLoading &&
    sections.decision.length === 0 &&
    sections.monitoring.length === 0 &&
    !hasNextPage;

  return (
    <div className="mx-auto flex w-full max-w-3xl flex-col gap-4 px-6 py-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="flex flex-col gap-0.5">
          <h1 className="font-semibold text-[15px] text-gray-12">Inbox</h1>
          <p className="text-[12.5px] text-gray-11">
            Issues and opportunities found in your product, ready to review
          </p>
        </div>
        <Button
          type="button"
          variant="outline"
          size="sm"
          onClick={() => navigateToAgents()}
        >
          Configure agents
        </Button>
      </div>

      <div className="flex flex-wrap items-center justify-end gap-2">
        <div className="flex items-center gap-2">
          <InboxScopeSelect />
        </div>
      </div>

      <InboxSearchFilterBar searchPlaceholder="Search reports…" />

      {isLoading && scopedReports.length === 0 ? (
        <div aria-hidden className="flex flex-col gap-2 pt-2">
          {[70, 55, 80, 60].map((width) => (
            <div key={width} className="flex items-center gap-3 py-2">
              <Skeleton className="h-4" style={{ width: `${width}%` }} />
            </div>
          ))}
        </div>
      ) : isEmpty ? (
        <Empty className="mx-auto max-w-md py-16">
          <EmptyHeader>
            <EmptyMedia variant="icon">
              <EnvelopeSimpleIcon size={24} />
            </EmptyMedia>
            <EmptyTitle>Nothing to review</EmptyTitle>
            <EmptyDescription>
              Reports show up here as your agents find things worth acting on.
            </EmptyDescription>
          </EmptyHeader>
        </Empty>
      ) : (
        <>
          <InboxSection
            title="Needs a decision"
            reports={sections.decision}
            countsComplete={countsComplete}
            caption={
              decisionPrCount > 0
                ? `${decisionPrCount} with a PR to review`
                : undefined
            }
            emptyNote="Nothing waiting on you."
          />
          <InboxSection
            title="Monitoring"
            reports={sections.monitoring}
            countsComplete={countsComplete}
          />
          <ResolvedSection />
          {isFetchingNextPage && (
            <div className="flex justify-center py-2">
              <Spinner />
            </div>
          )}
        </>
      )}
    </div>
  );
}

// A section header + capped rows. "Needs a decision" renders even when empty
// (the page's whole question deserves an explicit answer); others only with
// content.
function InboxSection({
  title,
  reports,
  countsComplete = true,
  caption,
  emptyNote,
}: {
  title: string;
  reports: SignalReport[];
  /** False while pages are still loading (or capped): the count wears a "+". */
  countsComplete?: boolean;
  /** Secondary breakdown shown after the count (e.g. "37 with a PR to review"). */
  caption?: string;
  emptyNote?: string;
}) {
  const [expanded, setExpanded] = useState(false);
  if (reports.length === 0 && !emptyNote) return null;
  const visible = expanded ? reports : reports.slice(0, SECTION_PREVIEW_LIMIT);
  const hidden = reports.length - visible.length;
  return (
    <section className="flex flex-col gap-1.5">
      <h2 className="flex items-baseline gap-2 border-(--gray-5) border-b pb-1 font-medium text-[11px] text-gray-10 uppercase tracking-wide">
        {title}
        <span className="tabular-nums">
          ({reports.length}
          {countsComplete ? "" : "+"})
        </span>
        {caption && (
          <span className="font-normal normal-case tracking-normal">
            · {caption}
          </span>
        )}
      </h2>
      {reports.length === 0 ? (
        <p className="px-1 py-2 text-[12.5px] text-gray-10">{emptyNote}</p>
      ) : (
        <div className="flex flex-col gap-1">
          {visible.map((report) => (
            <InboxReportRow key={report.id} report={report} />
          ))}
          {hidden > 0 && (
            <Button
              type="button"
              variant="link-muted"
              size="sm"
              className="self-center text-gray-10"
              onClick={() => setExpanded(true)}
            >
              Show more ({hidden})
            </Button>
          )}
        </div>
      )}
    </section>
  );
}

// A row carries what the old inbox's cards proved useful: the humanized
// title, one line of the summary's tl;dr (deciding without opening), where it
// came from, reviewers, the PR when there is one, and archive on hover — at
// row density rather than card height.
function InboxReportRow({ report }: { report: SignalReport }) {
  const products = (report.source_products ?? [])
    .map((product) => humanizeIdentifier(product).toLowerCase())
    .join(" · ");
  const headline = useMemo(
    () => deriveHeadline(report.summary),
    [report.summary],
  );
  const pr = report.implementation_pr_url
    ? parsePrUrl(report.implementation_pr_url)
    : null;
  const { actionButton: archiveButton, dialog: archiveDialog } =
    useInboxReportDismissAction(report);
  return (
    <>
      <div
        role="button"
        tabIndex={0}
        onClick={() => navigateToInboxReportDetail(report.id)}
        onKeyDown={(event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            navigateToInboxReportDetail(report.id);
          }
        }}
        className="group flex w-full cursor-pointer items-center gap-3 rounded-(--radius-2) border border-border bg-(--color-panel-solid) px-3 py-2 text-left transition hover:border-(--gray-6) hover:bg-(--gray-2)"
      >
        <div className="flex min-w-0 flex-1 flex-col gap-0.5">
          <span className="flex items-center gap-1.5">
            <span className="truncate font-medium text-[13px] text-gray-12">
              {humanizeReportTitle(report.title, "Untitled report")}
            </span>
          </span>
          {headline && (
            <span className="line-clamp-1 text-[12px] text-gray-11">
              {headline}
            </span>
          )}
          <span className="flex items-center gap-1.5 text-[11.5px] text-gray-10">
            {products && <span className="truncate">{products}</span>}
            <RelativeTimestamp
              timestamp={report.created_at}
              className="shrink-0 text-[11.5px]"
            />
          </span>
        </div>
        <span className="flex shrink-0 items-center gap-2">
          <SuggestedReviewerAvatarStack report={report} />
          <SignalReportPriorityBadge priority={report.priority} />
          <span className="font-mono text-[12px] text-gray-11 tabular-nums">
            {report.signal_count} signal{report.signal_count === 1 ? "" : "s"}
          </span>
          {/* Acting on a row must not also open it. */}
          {/* biome-ignore lint/a11y/noStaticElementInteractions: propagation guard for the buttons inside, not interactive itself */}
          <span
            onClick={(event) => event.stopPropagation()}
            onKeyDown={(event) => event.stopPropagation()}
            className="flex items-center gap-1.5"
          >
            {pr && (
              <button
                type="button"
                onClick={() => {
                  if (report.implementation_pr_url) {
                    openExternalUrl(report.implementation_pr_url);
                  }
                }}
                title={
                  report.implementation_pr_merged
                    ? "This report's earlier PR merged, but evidence kept arriving"
                    : "Open the pull request on GitHub"
                }
                className="flex items-center gap-1 rounded border border-(--gray-6) px-1.5 py-0.5 font-mono text-[11px] text-gray-11 hover:bg-(--gray-3) hover:text-gray-12"
              >
                <GitPullRequestIcon size={11} />
                #{pr.number}
                {report.implementation_pr_merged ? " merged" : ""}
              </button>
            )}
            {report.implementation_pr_url && !report.implementation_pr_merged && (
              <Button
                type="button"
                variant="primary"
                size="sm"
                onClick={() => navigateToInboxReportDetail(report.id)}
              >
                Review
              </Button>
            )}
            <span className="opacity-0 transition-opacity focus-within:opacity-100 group-hover:opacity-100">
              {archiveButton}
            </span>
          </span>
        </span>
      </div>
      {archiveDialog}
    </>
  );
}

// Archived and resolved reports come from their own server-side fetch, so the
// section fetches lazily on first expand and stays collapsed by default.
function ResolvedSection() {
  const [expanded, setExpanded] = useState(false);
  const {
    allReports,
    isLoading,
    hasNextPage,
    fetchNextPage,
    isFetchingNextPage,
  } = useInboxReportsInfinite(
    { status: INBOX_DISMISSED_STATUS_FILTER, ordering: "-updated_at" },
    { enabled: expanded, pageSize: 25 },
  );
  return (
    <section className="flex flex-col gap-1.5">
      <button
        type="button"
        onClick={() => setExpanded((v) => !v)}
        className="flex items-baseline gap-2 border-(--gray-5) border-b pb-1 text-left font-medium text-[11px] text-gray-10 uppercase tracking-wide"
      >
        Resolved
        <CaretDownIcon
          size={11}
          className={expanded ? "rotate-180 self-center" : "self-center"}
        />
      </button>
      {expanded &&
        (isLoading ? (
          <div className="flex justify-center py-3">
            <Spinner />
          </div>
        ) : allReports.length === 0 ? (
          <p className="px-1 py-2 text-[12.5px] text-gray-10">
            Nothing resolved yet.
          </p>
        ) : (
          <div className="flex flex-col gap-1">
            {allReports.map((report) => (
              <InboxReportRow key={report.id} report={report} />
            ))}
            {hasNextPage && (
              <Button
                type="button"
                variant="link-muted"
                size="sm"
                className="self-center text-gray-10"
                disabled={isFetchingNextPage}
                onClick={() => fetchNextPage()}
              >
                {isFetchingNextPage ? <Spinner /> : "Show more"}
              </Button>
            )}
          </div>
        ))}
    </section>
  );
}
