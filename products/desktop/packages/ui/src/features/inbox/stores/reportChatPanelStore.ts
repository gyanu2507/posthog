import { create } from "zustand";

interface ReportChatPanelState {
  /** Whether the chat dock is showing beside the open report. */
  open: boolean;
  width: number;
  /**
   * Task ids of discussions started this session, per report. Bridges the gap
   * between creating the discussion task and its task_run artefact appearing
   * in the report's artefact list (the durable association).
   */
  startedTaskIdByReport: Record<string, string>;
  /**
   * A highlighted passage waiting to be quoted into the report's chat
   * composer. Written by the selection affordance, consumed once by the panel.
   */
  pendingQuoteByReport: Record<string, string>;
  setOpen: (open: boolean) => void;
  setWidth: (width: number) => void;
  rememberStartedTask: (reportId: string, taskId: string) => void;
  setPendingQuote: (reportId: string, quote: string) => void;
  /**
   * Read and clear in one call. Consumers take rather than read+clear:
   * effects run twice under StrictMode, and a second take returns null
   * instead of pasting the quote again.
   */
  takePendingQuote: (reportId: string) => string | null;
}

export const useReportChatPanelStore = create<ReportChatPanelState>((set, get) => ({
  open: false,
  width: 420,
  startedTaskIdByReport: {},
  pendingQuoteByReport: {},
  setOpen: (open) => set({ open }),
  setWidth: (width) => set({ width }),
  rememberStartedTask: (reportId, taskId) =>
    set((state) => ({
      startedTaskIdByReport: {
        ...state.startedTaskIdByReport,
        [reportId]: taskId,
      },
    })),
  setPendingQuote: (reportId, quote) =>
    set((state) => ({
      pendingQuoteByReport: {
        ...state.pendingQuoteByReport,
        [reportId]: quote,
      },
    })),
  takePendingQuote: (reportId) => {
    const quote = get().pendingQuoteByReport[reportId] ?? null;
    if (quote !== null) {
      set((state) => {
        const { [reportId]: _, ...rest } = state.pendingQuoteByReport;
        return { pendingQuoteByReport: rest };
      });
    }
    return quote;
  },
}));
