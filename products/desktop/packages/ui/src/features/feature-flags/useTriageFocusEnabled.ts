import { TRIAGE_FOCUS_FLAG } from "@posthog/shared";
import { useFeatureFlag } from "@posthog/ui/features/feature-flags/useFeatureFlag";

/** Off by default everywhere, dev included, until focus mode stabilizes. */
export function useTriageFocusEnabled(): boolean {
  return useFeatureFlag(TRIAGE_FOCUS_FLAG, false);
}
