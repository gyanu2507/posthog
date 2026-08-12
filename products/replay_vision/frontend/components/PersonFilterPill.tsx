import { LemonButtonWithDropdown, LemonInput } from '@posthog/lemon-ui'

/** Compact summary for the pill face, so an active filter is readable without opening the dropdown. */
export function personFilterLabel(value: string): string {
    const trimmed = value.trim()
    return trimmed ? `Person ${trimmed}` : 'Person'
}

/** Person email filter as a pill, so the toolbar keeps a single free-text search input. */
export function PersonFilterPill({
    value,
    onChange,
}: {
    value: string
    onChange: (value: string) => void
}): JSX.Element {
    return (
        <LemonButtonWithDropdown
            type="secondary"
            size="small"
            data-attr="vision-observations-person-filter"
            dropdown={{
                closeOnClickInside: false,
                overlay: (
                    <div className="p-1 w-64">
                        <LemonInput
                            type="search"
                            size="small"
                            placeholder="Person email"
                            autoFocus
                            fullWidth
                            value={value}
                            onChange={onChange}
                        />
                    </div>
                ),
            }}
        >
            <span className="truncate max-w-48">{personFilterLabel(value)}</span>
        </LemonButtonWithDropdown>
    )
}
