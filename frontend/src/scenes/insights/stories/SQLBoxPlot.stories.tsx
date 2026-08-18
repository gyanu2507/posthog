import { Meta, StoryObj } from '@storybook/react'

import { FEATURE_FLAGS } from 'lib/constants'
import { createInsightStory } from 'scenes/insights/__mocks__/createInsightScene'

import { mswDecorator } from '~/mocks/browser'

import __sqlBoxPlot from '../../../mocks/fixtures/api/projects/team_id/insights/sqlBoxPlot.json'

type Story = StoryObj<{}>

const meta: Meta = {
    title: 'Scenes-App/Insights/SQLBoxPlot',
    parameters: {
        layout: 'fullscreen',
        featureFlags: [FEATURE_FLAGS.SQL_BOX_PLOT_INSIGHT],
        testOptions: {
            snapshotBrowsers: ['chromium'],
            viewport: { width: 1300, height: 720 },
            waitForSelector: '[data-attr="sql-box-plot"]',
        },
        viewMode: 'story',
        mockDate: '2026-02-02',
    },
    decorators: [
        mswDecorator({
            get: {
                '/api/projects/:team_id/groups_types': [],
            },
        }),
    ],
}

export default meta

export const GroupedSeries: Story = createInsightStory(__sqlBoxPlot as any)
