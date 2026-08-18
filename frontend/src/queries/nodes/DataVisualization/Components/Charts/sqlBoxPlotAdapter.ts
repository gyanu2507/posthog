import type { BoxPlotSeries } from '@posthog/quill-charts'

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

type StatisticSetting = keyof Pick<
    BoxPlotSettings,
    'minColumn' | 'p25Column' | 'medianColumn' | 'meanColumn' | 'p75Column' | 'maxColumn'
>

const statisticSettings: { setting: StatisticSetting; label: string; aliases: string[] }[] = [
    { setting: 'minColumn', label: 'Minimum', aliases: ['min', 'minimum'] },
    { setting: 'p25Column', label: '25th percentile', aliases: ['p25', 'q1'] },
    { setting: 'medianColumn', label: 'Median', aliases: ['median', 'p50'] },
    { setting: 'meanColumn', label: 'Mean', aliases: ['mean', 'avg', 'average'] },
    { setting: 'p75Column', label: '75th percentile', aliases: ['p75', 'q3'] },
    { setting: 'maxColumn', label: 'Maximum', aliases: ['max', 'maximum'] },
]

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

    for (const statistic of statisticSettings) {
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
    const statisticColumns = statisticSettings.map((statistic) => ({
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
    const seriesLabels: string[] = []
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

        const values = statisticColumns.map(({ column }) => finiteNumber(row[column!.dataIndex]))
        if (values.some((value) => value === null)) {
            return emptyModel(`Row ${rowIndex + 1} has a missing or non-numeric box plot statistic.`)
        }

        const [min, p25, median, mean, p75, max] = values as [number, number, number, number, number, number]
        if (!(min <= p25 && p25 <= median && median <= p75 && p75 <= max)) {
            return emptyModel(
                `Row ${rowIndex + 1} has statistics in the wrong order. Expected min <= p25 <= median <= p75 <= max.`
            )
        }
        if (mean < min || mean > max) {
            return emptyModel(`Row ${rowIndex + 1} has a mean outside its minimum and maximum.`)
        }

        if (!labels.includes(label)) {
            labels.push(label)
        }
        if (!seriesLabels.includes(seriesLabel)) {
            seriesLabels.push(seriesLabel)
        }

        const iqr = p75 - p25
        const excludeOutliers = settings.excludeOutliers !== false
        const datum = {
            min: excludeOutliers ? Math.max(min, p25 - 1.5 * iqr) : min,
            p25,
            median,
            mean,
            p75,
            max: excludeOutliers ? Math.min(max, p75 + 1.5 * iqr) : max,
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
