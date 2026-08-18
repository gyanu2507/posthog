import '@testing-library/jest-dom'

import { cleanup, render, screen, waitFor } from '@testing-library/react'

import type { BoxPlotSeries } from '@posthog/quill-charts'

import { FEATURE_FLAGS } from 'lib/constants'
import { featureFlagLogic } from 'lib/logic/featureFlagLogic'

import { DataVisualizationNode, HogQLQueryResponse, NodeKind } from '~/queries/schema/schema-general'
import { initKeaTests } from '~/test/init'
import { ChartDisplayType } from '~/types'

import { DataTableVisualization } from './DataVisualization'

type LemonTableMockProps = {
    embedded?: boolean
    allowContentScroll?: boolean
}

let mockLatestLemonTableProps: LemonTableMockProps | null = null
let mockLatestBoxPlotProps: { labels: string[]; series: BoxPlotSeries[] } | null = null
const mockLemonTable = jest.fn((props: LemonTableMockProps): null => {
    mockLatestLemonTableProps = props
    return null
})

jest.mock('@posthog/quill-charts', () => ({
    ...jest.requireActual('@posthog/quill-charts'),
    BoxPlot: (props: { labels: string[]; series: BoxPlotSeries[] }): JSX.Element => {
        mockLatestBoxPlotProps = props
        return <div data-attr="mock-sql-box-plot" />
    },
}))

jest.mock('@posthog/lemon-ui', () => ({
    ...jest.requireActual('@posthog/lemon-ui'),
    LemonTable: (props: Record<string, unknown>): null => {
        mockLemonTable(props)
        return null
    },
}))

describe('DataTableVisualization', () => {
    const query: DataVisualizationNode = {
        kind: NodeKind.DataVisualizationNode,
        source: {
            kind: NodeKind.HogQLQuery,
            query: 'select number from numbers(2)',
        },
        display: ChartDisplayType.ActionsTable,
    }

    const cachedResults: HogQLQueryResponse<number[][]> = {
        results: [[1], [2]],
        columns: ['number'],
        types: [['number', 'Int64']],
    }

    const boxPlotQuery: DataVisualizationNode = {
        ...query,
        display: ChartDisplayType.BoxPlot,
        chartSettings: {
            boxPlot: {
                xAxisColumn: 'bucket',
                seriesColumn: 'series',
                minColumn: 'min',
                p25Column: 'p25',
                medianColumn: 'median',
                meanColumn: 'mean',
                p75Column: 'p75',
                maxColumn: 'max',
                excludeOutliers: false,
            },
        },
    }

    const boxPlotResults: HogQLQueryResponse = {
        results: [
            ['Mon', 'Free', 1, 2, 3, 4, 5, 6],
            ['Mon', 'Paid', 7, 8, 9, 10, 11, 12],
        ],
        columns: ['bucket', 'series', 'min', 'p25', 'median', 'mean', 'p75', 'max'],
        types: [
            ['bucket', 'String'],
            ['series', 'String'],
            ['min', 'Float64'],
            ['p25', 'Float64'],
            ['median', 'Float64'],
            ['mean', 'Float64'],
            ['p75', 'Float64'],
            ['max', 'Float64'],
        ],
    }

    beforeEach(() => {
        initKeaTests()
        featureFlagLogic.mount()
        featureFlagLogic.actions.setFeatureFlags([], {})
        mockLatestLemonTableProps = null
        mockLatestBoxPlotProps = null
        mockLemonTable.mockClear()
    })

    afterEach(() => {
        cleanup()
        featureFlagLogic.unmount()
    })

    test.each([true, false])('renders saved SQL box plots when the feature flag is %s', async (flagEnabled) => {
        featureFlagLogic.actions.setFeatureFlags(flagEnabled ? [FEATURE_FLAGS.SQL_BOX_PLOT_INSIGHT] : [], {
            [FEATURE_FLAGS.SQL_BOX_PLOT_INSIGHT]: flagEnabled,
        })

        render(
            <DataTableVisualization
                uniqueKey={`data-visualization-box-plot-${flagEnabled}`}
                query={boxPlotQuery}
                setQuery={jest.fn()}
                cachedResults={boxPlotResults}
                readOnly
                embedded
            />
        )

        await screen.findByTestId('mock-sql-box-plot')
        expect(mockLatestBoxPlotProps).toMatchObject({
            labels: ['Mon'],
            series: [
                { key: 'Free', label: 'Free' },
                { key: 'Paid', label: 'Paid' },
            ],
        })
    })

    it('explains how to fix invalid SQL box plot results', async () => {
        const boxPlotQuery: DataVisualizationNode = {
            ...query,
            display: ChartDisplayType.BoxPlot,
            chartSettings: { boxPlot: {} },
        }

        render(
            <DataTableVisualization
                uniqueKey="data-visualization-invalid-box-plot"
                query={boxPlotQuery}
                setQuery={jest.fn()}
                cachedResults={{ results: [[1]], columns: ['value'], types: [['value', 'Float64']] }}
                readOnly
                embedded
            />
        )

        expect(await screen.findByText('Select a column for Minimum.')).toBeInTheDocument()
    })

    test.each([
        { embedded: true, expectedAllowContentScroll: true },
        { embedded: false, expectedAllowContentScroll: false },
    ])(
        'sets table scroll mode to $expectedAllowContentScroll when embedded is $embedded',
        async ({ embedded, expectedAllowContentScroll }) => {
            render(
                <DataTableVisualization
                    uniqueKey={`data-visualization-scroll-${embedded}`}
                    query={query}
                    setQuery={jest.fn()}
                    cachedResults={cachedResults}
                    readOnly
                    embedded={embedded}
                />
            )

            await waitFor(() => {
                if (!mockLatestLemonTableProps) {
                    throw new Error('Expected LemonTable to render')
                }
            })

            if (!mockLatestLemonTableProps) {
                throw new Error('Expected LemonTable props to be recorded')
            }
            expect(mockLatestLemonTableProps.embedded).toBe(embedded)
            expect(mockLatestLemonTableProps.allowContentScroll).toBe(expectedAllowContentScroll)
        }
    )
})
