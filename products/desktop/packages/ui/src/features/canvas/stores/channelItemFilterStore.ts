import {
  type ChannelItemFilters,
  type ChannelItemGrouping,
  type ChannelItemSort,
  DEFAULT_CHANNEL_ITEM_FILTERS,
  DEFAULT_CHANNEL_ITEM_GROUPING,
  DEFAULT_CHANNEL_ITEM_SORT,
  sanitizeChannelItemFilters,
  sanitizeChannelItemGrouping,
  sanitizeChannelItemSort,
} from "@posthog/core/canvas/channelItems";
import { create } from "zustand";
import { persist } from "zustand/middleware";

interface ChannelItemFilterStore {
  filters: ChannelItemFilters;
  sort: ChannelItemSort;
  grouping: ChannelItemGrouping;
  /**
   * One field at a time. Writing the whole object back would carry the narrowed
   * values the menu displays and drop a choice made under another tab.
   */
  setFilter: <K extends keyof ChannelItemFilters>(
    key: K,
    value: ChannelItemFilters[K],
  ) => void;
  clearFilters: () => void;
  setSort: (sort: ChannelItemSort) => void;
  setGrouping: (grouping: ChannelItemGrouping) => void;
}

/**
 * How the space list is narrowed and ordered, as a per-device preference. It
 * already carried across a space switch, and a reload is no more of a request
 * to reset it than that was — a filter you set is a filter you should find.
 */
export const useChannelItemFilterStore = create<ChannelItemFilterStore>()(
  persist(
    (set) => ({
      filters: DEFAULT_CHANNEL_ITEM_FILTERS,
      sort: DEFAULT_CHANNEL_ITEM_SORT,
      grouping: DEFAULT_CHANNEL_ITEM_GROUPING,
      setFilter: (key, value) =>
        set((state) => ({ filters: { ...state.filters, [key]: value } })),
      clearFilters: () => set({ filters: DEFAULT_CHANNEL_ITEM_FILTERS }),
      setSort: (sort) => set({ sort }),
      setGrouping: (grouping) => set({ grouping }),
    }),
    {
      name: "channel-item-filter-storage",
      partialize: (state) => ({
        filters: state.filters,
        sort: state.sort,
        grouping: state.grouping,
      }),
      merge: (persisted, current) => {
        const stored = persisted as {
          filters?: unknown;
          sort?: unknown;
          grouping?: unknown;
        } | null;
        return {
          ...current,
          filters: sanitizeChannelItemFilters(stored?.filters),
          sort: sanitizeChannelItemSort(stored?.sort),
          grouping: sanitizeChannelItemGrouping(stored?.grouping),
        };
      },
    },
  ),
);
