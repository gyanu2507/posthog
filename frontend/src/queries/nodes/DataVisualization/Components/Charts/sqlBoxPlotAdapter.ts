import type { BoxPlotDatum, BoxPlotSeries } from '@posthog/quill-charts'

import { BoxPlotSettings } from '~/queries/schema/schema-general'

interface BoxPlotColumn {
    name: string
    dataIndex: number
    type: { isNumerical: boolean }
}

export interface SqlBoxPlotModel {
    labels: string[]
    series: BoxPlotSeries[]
    error: string | null
}

export type BoxPlotStatisticColumn = keyof Pick<
    BoxPlotSettings,
    'minColumn' | 'p25Column' | 'medianColumn' | 'meanColumn' | 'p75Column' | 'maxColumn'
>

type BoxPlotValue = keyof Pick<BoxPlotDatum, 'min' | 'p25' | 'median' | 'mean' | 'p75' | 'max'>

export const BOX_PLOT_STATISTICS: {
    setting: BoxPlotStatisticColumn
    value: BoxPlotValue
    label: string
    aliases: string[]
}[] = [
    { setting: 'minColumn', value: 'min', label: 'Minimum', aliases: ['min', 'minimum'] },
    { setting: 'p25Column', value: 'p25', label: '25th percentile', aliases: ['p25', 'q1'] },
    { setting: 'medianColumn', value: 'median', label: 'Median', aliases: ['median', 'p50'] },
    { setting: 'meanColumn', value: 'mean', label: 'Mean', aliases: ['mean', 'avg', 'average'] },
    { setting: 'p75Column', value: 'p75', label: '75th percentile', aliases: ['p75', 'q3'] },
    { setting: 'maxColumn', value: 'max', label: 'Maximum', aliases: ['max', 'maximum'] },
]

const MAX_BOX_PLOT_CELLS = 10_000

const emptyModel = (error: string | null = null): SqlBoxPlotModel => ({ labels: [], series: [], error })

const findColumn = (
    columns: BoxPlotColumn[],
    name: string | undefined,
    numerical = false
): BoxPlotColumn | undefined => {
    if (!name) {
        return undefined
    }
    return columns.find((column) => column.name === name && (!numerical || column.type.isNumerical))
}

const findAliasedColumn = (columns: BoxPlotColumn[], aliases: string[], numerical = false): BoxPlotColumn | undefined =>
    columns.find((column) => aliases.includes(column.name.toLowerCase()) && (!numerical || column.type.isNumerical))

export const getAutoBoxPlotSettings = (columns: BoxPlotColumn[], current: BoxPlotSettings = {}): BoxPlotSettings => {
    const next = { ...current }

    if (!findColumn(columns, current.xAxisColumn)) {
        next.xAxisColumn = findAliasedColumn(columns, ['label', 'bucket', 'date', 'day'])?.name
    }
    if (!findColumn(columns, current.seriesColumn)) {
        next.seriesColumn = findAliasedColumn(columns, ['series', 'breakdown'])?.name
    }

    for (const statistic of BOX_PLOT_STATISTICS) {
        if (!findColumn(columns, current[statistic.setting], true)) {
            next[statistic.setting] = findAliasedColumn(columns, statistic.aliases, true)?.name
        }
    }

    return next
}

const finiteNumber = (value: unknown): number | null => {
    if (value === null || value === undefined || value === '') {
        return null
    }
    const number = typeof value === 'number' ? value : Number(value)
    return Number.isFinite(number) ? number : null
}

export const buildSqlBoxPlotModel = (
    rows: unknown[][],
    columns: BoxPlotColumn[],
    settings: BoxPlotSettings
): SqlBoxPlotModel => {
    const statisticColumns = BOX_PLOT_STATISTICS.map((statistic) => ({
        ...statistic,
        column: findColumn(columns, settings[statistic.setting], true),
    }))
    const missingStatistic = statisticColumns.find(({ column }) => !column)
    if (missingStatistic) {
        return emptyModel(`Select a column for ${missingStatistic.label}.`)
    }

    if (rows.length === 0) {
        return emptyModel()
    }

    const xAxisColumn = findColumn(columns, settings.xAxisColumn)
    const seriesColumn = findColumn(columns, settings.seriesColumn)
    if (!xAxisColumn && rows.length > 1) {
        return emptyModel('Select an X-axis column when the query returns more than one row.')
    }

    const labels: string[] = []
    const labelSet = new Set<string>()
    const seriesLabels: string[] = []
    const seriesLabelSet = new Set<string>()
    const dataBySeries = new Map<string, Map<string, BoxPlotSeries['data'][number]>>()
    const rowByPair = new Map<string, number>()

    for (const [rowIndex, row] of rows.entries()) {
        const label = xAxisColumn ? String(row[xAxisColumn.dataIndex] ?? '[No value]') : 'Distribution'
        const seriesLabel = seriesColumn ? String(row[seriesColumn.dataIndex] ?? '[No value]') : 'Distribution'
        const pairKey = JSON.stringify([label, seriesLabel])
        const previousRow = rowByPair.get(pairKey)
        if (previousRow !== undefined) {
            return emptyModel(
                `Rows ${previousRow + 1} and ${rowIndex + 1} use the same X-axis and series values. Return one row for each box.`
            )
        }
        rowByPair.set(pairKey, rowIndex)

        const nullableValues = Object.fromEntries(
            statisticColumns.map((statistic) => [statistic.value, finiteNumber(row[statistic.column!.dataIndex])])
        ) as Record<BoxPlotValue, number | null>
        if (Object.values(nullableValues).some((value) => value === null)) {
            return emptyModel(`Row ${rowIndex + 1} has a missing or non-numeric box plot statistic.`)
        }

        const values = nullableValues as Record<BoxPlotValue, number>
        if (
            !(
                values.min <= values.p25 &&
                values.p25 <= values.median &&
                values.median <= values.p75 &&
                values.p75 <= values.max
            )
        ) {
            return emptyModel(
                `Row ${rowIndex + 1} has statistics in the wrong order. Expected min <= p25 <= median <= p75 <= max.`
            )
        }
        if (values.mean < values.min || values.mean > values.max) {
            return emptyModel(`Row ${rowIndex + 1} has a mean outside its minimum and maximum.`)
        }
        if (!labelSet.has(label)) {
            labels.push(label)
            labelSet.add(label)
        }
        if (!seriesLabelSet.has(seriesLabel)) {
            seriesLabels.push(seriesLabel)
            seriesLabelSet.add(seriesLabel)
        }
        if (labelSet.size * seriesLabelSet.size > MAX_BOX_PLOT_CELLS) {
            return emptyModel('The box plot has too many X-axis and series combinations. Reduce the query result.')
        }

        const iqr = values.p75 - values.p25
        const excludeOutliers = settings.excludeOutliers !== false
        const datum = {
            ...values,
            min: excludeOutliers ? Math.max(values.min, values.p25 - 1.5 * iqr) : values.min,
            max: excludeOutliers ? Math.min(values.max, values.p75 + 1.5 * iqr) : values.max,
        }
        const seriesData = dataBySeries.get(seriesLabel) ?? new Map()
        seriesData.set(label, datum)
        dataBySeries.set(seriesLabel, seriesData)
    }

    return {
        labels,
        series: seriesLabels.map((seriesLabel) => ({
            key: seriesLabel,
            label: seriesLabel,
            data: labels.map((label) => dataBySeries.get(seriesLabel)?.get(label) ?? null),
        })),
        error: null,
    }
}
