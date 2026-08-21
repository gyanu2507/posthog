import {
  ArchiveIcon,
  ArrowSquareOutIcon,
  CopyIcon,
  FileTextIcon,
} from "@phosphor-icons/react";
import {
  Button,
  ContextMenu,
  ContextMenuContent,
  ContextMenuItem,
  ContextMenuTrigger,
} from "@posthog/quill";
import type { SignalReport } from "@posthog/shared/types";
import { copyInboxReportLink } from "@posthog/ui/features/inbox/utils/copyInboxReportLink";
import { openExternalUrl } from "@posthog/ui/shell/openExternal";
import type { ComponentType, ReactNode } from "react";

export interface ReportRowMenuProps {
  report: SignalReport;
  onOpen: () => void;
  /** Absent on archived rows, which restore instead of archiving again. */
  onArchive?: () => void;
}

// The context menu and the hover card draw the same items with different
// primitives, so the list is written once against this shape (the same
// pattern TaskRowMenu uses for sessions).
interface MenuParts {
  Item: ComponentType<{
    children: ReactNode;
    variant?: "default" | "destructive";
    onClick?: () => void;
  }>;
}

function ReportRowMenuItems({
  parts,
  menu,
}: {
  parts: MenuParts;
  menu: ReportRowMenuProps;
}) {
  const { report, onOpen, onArchive } = menu;
  return (
    <>
      <parts.Item onClick={onOpen}>
        <FileTextIcon size={14} />
        Open report
      </parts.Item>
      {report.implementation_pr_url && (
        <parts.Item
          onClick={() => {
            if (report.implementation_pr_url) {
              openExternalUrl(report.implementation_pr_url);
            }
          }}
        >
          <ArrowSquareOutIcon size={14} />
          Open PR on GitHub
        </parts.Item>
      )}
      <parts.Item onClick={() => copyInboxReportLink(report)}>
        <CopyIcon size={14} />
        Copy link
      </parts.Item>
      {onArchive && (
        <parts.Item variant="destructive" onClick={onArchive}>
          <ArchiveIcon size={14} />
          Archive…
        </parts.Item>
      )}
    </>
  );
}

const CONTEXT_PARTS: MenuParts = {
  Item: ContextMenuItem,
};

/** The same actions on right-click, wrapping the row. */
export function ReportRowContextMenu({
  menu,
  children,
}: {
  menu: ReportRowMenuProps;
  children: ReactNode;
}) {
  return (
    <ContextMenu>
      <ContextMenuTrigger render={<div className="min-w-0" />}>
        {children}
      </ContextMenuTrigger>
      <ContextMenuContent className="w-56">
        <ReportRowMenuItems parts={CONTEXT_PARTS} menu={menu} />
      </ContextMenuContent>
    </ContextMenu>
  );
}

/** The same actions inside the hover card, as full-width buttons. */
export function ReportRowMenuList({
  menu,
  onAction,
}: {
  menu: ReportRowMenuProps;
  onAction: () => void;
}) {
  const parts: MenuParts = {
    Item: ({ children, variant, onClick }) => (
      <Button
        variant={variant === "destructive" ? "destructive" : "default"}
        left
        className="w-full"
        onClick={() => {
          onClick?.();
          onAction();
        }}
      >
        {children}
      </Button>
    ),
  };
  return (
    <div className="flex flex-col">
      <ReportRowMenuItems parts={parts} menu={menu} />
    </div>
  );
}
