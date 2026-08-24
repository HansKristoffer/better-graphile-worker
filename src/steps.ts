import type { JobHelpers } from 'graphile-worker'
import type { JobSpan } from './hooks'
import { NonRetriableError, StepSerializationError } from './errors'
import { extractStepCache, withStepCache, type StepCache } from './payload'
import { assertValidSchemaName } from './schema-name'

export type JobStep = {
	run<T>(id: string, fn: () => Promise<T> | T): Promise<T>
}

export type StepStore = {
	get(id: string): { output: unknown } | undefined
	set(id: string, output: unknown): Promise<void>
}

export function serializeStepOutput(stepId: string, value: unknown): unknown {
	if (value === undefined) return null
	try {
		return JSON.parse(JSON.stringify(value))
	} catch (error) {
		throw new StepSerializationError(stepId, {
			cause: error instanceof Error ? error : undefined
		})
	}
}

export function createStepRunner(options: {
	store: StepStore
	span: JobSpan
}): JobStep {
	return {
		async run(id, fn) {
			if (typeof id !== 'string' || id.length === 0) {
				throw new NonRetriableError('Step id must be a non-empty string')
			}

			const cached = options.store.get(id)
			if (cached) {
				options.span.addEvent('step.cache_hit', { 'step.id': id })
				return cached.output as never
			}

			try {
				const result = await fn()
				const serialized = serializeStepOutput(id, result)
				await options.store.set(id, serialized)
				options.span.addEvent('step.completed', { 'step.id': id })
				return serialized as never
			} catch (error) {
				options.span.addEvent('step.failed', { 'step.id': id })
				throw error
			}
		}
	}
}

export function createMemoryStepStore(initial: StepCache = {}): StepStore {
	const cache: StepCache = { ...initial }
	return {
		get(id) {
			return cache[id]
		},
		async set(id, output) {
			cache[id] = { output }
		}
	}
}

export function createPgStepStore(options: {
	helpers: JobHelpers
	schema: string
	jobId: string
	rawPayload: unknown
}): StepStore {
	const schema = assertValidSchemaName(options.schema)
	const cache: StepCache = { ...extractStepCache(options.rawPayload) }
	const initialPayload = options.rawPayload

	return {
		get(id) {
			return cache[id]
		},
		async set(id, output) {
			cache[id] = { output }
			const nextPayload = withStepCache(initialPayload, cache)
			await options.helpers.withPgClient(async (client) => {
				await client.query(
					`UPDATE ${schema}._private_jobs
					SET payload = $2::jsonb, updated_at = NOW()
					WHERE id = $1`,
					[options.jobId, JSON.stringify(nextPayload)]
				)
			})
		}
	}
}
