import clsx from 'clsx'
import { useMemo } from 'react'

import { BoxPlot } from '@posthog/quill-charts'
import type { BoxPlotConfig } from '@posthog/quill-charts'

import { useChartConfig, useChartTheme } from 'lib/charts/hooks'

import { BoxPlotSettings, ChartSettings } from '~/queries/schema/schema-general'

import { makeChartErrorHandler } from 'products/product_analytics/frontend/insights/trends/shared/chartErrorHandler'

import { Column } from '../../dataVisualizationLogic'
import { buildSqlBoxPlotModel } from './sqlBoxPlotAdapter'

const handleChartError = makeChartErrorHandler('sql-box-plot')

export interface SqlBoxPlotProps {
    rows: unknown[][]
    columns: Column[]
    settings: BoxPlotSettings
    chartSettings: ChartSettings
    presetChartHeight?: boolean
    className?: string
}

export const SqlBoxPlot = ({
    rows,
    columns,
    settings,
    chartSettings,
    presetChartHeight,
    className,
}: SqlBoxPlotProps): JSX.Element => {
    const theme = useChartTheme()
    const model = useMemo(() => buildSqlBoxPlotModel(rows, columns, settings), [rows, columns, settings])
    const yAxisSettings = chartSettings.leftYAxisSettings
    const config = useChartConfig<BoxPlotConfig>(
        () => ({
            yScaleType: yAxisSettings?.scale === 'logarithmic' ? 'log' : 'linear',
            xAxisLabel: chartSettings.xAxisLabel,
            yAxisLabel: yAxisSettings?.label,
            hideXAxis: chartSettings.showXAxisTicks === false,
            hideYAxis: yAxisSettings?.showTicks === false,
            showGrid: yAxisSettings?.showGridLines ?? true,
            showAxisLines: {
                x: chartSettings.showXAxisBorder ?? true,
                y: chartSettings.showYAxisBorder ?? true,
            },
            tooltip: { pinnable: true, placement: 'cursor' },
            legend: { show: chartSettings.showLegend ?? false, position: 'top' },
        }),
        [chartSettings, yAxisSettings]
    )

    const containerClassName = clsx(
        className,
        'rounded bg-surface-primary flex flex-1 items-center justify-center p-3',
        { 'h-[60vh]': presetChartHeight, 'h-full': !presetChartHeight }
    )

    if (model.error) {
        return (
            <div className={containerClassName} data-attr="sql-box-plot-error">
                <span className="text-secondary text-sm">{model.error}</span>
            </div>
        )
    }

    if (model.series.length === 0) {
        return (
            <div className={containerClassName} data-attr="sql-box-plot-empty">
                <span className="text-secondary text-sm">No boxes to plot. Check that your query returns rows.</span>
            </div>
        )
    }

    return (
        <div
            className={clsx(
                className,
                'rounded bg-surface-primary w-full grow relative overflow-hidden flex flex-col p-3',
                { 'h-[60vh]': presetChartHeight, 'h-full': !presetChartHeight }
            )}
        >
            <BoxPlot
                series={model.series}
                labels={model.labels}
                theme={theme}
                config={config}
                dataAttr="sql-box-plot"
                onError={handleChartError}
            />
        </div>
    )
}
