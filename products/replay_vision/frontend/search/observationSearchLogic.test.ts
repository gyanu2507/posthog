import { router } from 'kea-router'
import { expectLogic } from 'kea-test-utils'

import { urls } from 'scenes/urls'

import { useMocks } from '~/mocks/jest'
import { initKeaTests } from '~/test/init'

import { observationSearchLogic } from './observationSearchLogic'

describe('observationSearchLogic', () => {
    let searchSpy: jest.Mock

    beforeEach(() => {
        searchSpy = jest.fn(() => [200, { results: [{ id: 'obs-1' }] }])
        useMocks({
            get: {
                '/api/projects/:team/vision/observations/search/': searchSpy,
            },
        })
        initKeaTests()
    })

    it.each([
        ['a scanner-scoped', 'scanner-1', 'scanner-1'],
        ['a cross-scanner', null, null],
    ])('%s search sends the right scope and stores ranked results', async (_name, scannerId, expectedScope) => {
        const logic = observationSearchLogic({ scannerId })
        logic.mount()
        logic.actions.setQuery('confused users')
        await expectLogic(logic, () => logic.actions.search()).toFinishAllListeners()

        expect(searchSpy).toHaveBeenCalledTimes(1)
        const requestUrl = new URL(searchSpy.mock.calls[0][0].request.url)
        expect(requestUrl.searchParams.get('q')).toBe('confused users')
        expect(requestUrl.searchParams.get('scanner_id')).toBe(expectedScope)
        expect(logic.values.results?.map((r: { id: string }) => r.id)).toEqual(['obs-1'])
        logic.unmount()
    })

    it('a blank query never reaches the API', async () => {
        const logic = observationSearchLogic({ scannerId: null })
        logic.mount()
        logic.actions.setQuery('   ')
        await expectLogic(logic, () => logic.actions.search()).toFinishAllListeners()

        expect(searchSpy).not.toHaveBeenCalled()
        expect(logic.values.searching).toBe(false)
        logic.unmount()
    })

    it('a deep-linked q runs the search once, not on every navigation', async () => {
        const logic = observationSearchLogic({ scannerId: null })
        logic.mount()
        router.actions.push(urls.replayVision(), { tab: 'search', q: 'rage clicks' })
        await expectLogic(logic).toFinishAllListeners()
        expect(searchSpy).toHaveBeenCalledTimes(1)

        router.actions.push(urls.replayVision(), { tab: 'search', q: 'rage clicks' })
        await expectLogic(logic).toFinishAllListeners()
        expect(searchSpy).toHaveBeenCalledTimes(1)
        logic.unmount()
    })
})
