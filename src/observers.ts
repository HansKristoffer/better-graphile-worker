import type {
	BetterWorkerHooks,
	CreateLoggerOptions,
	JobAttributes,
	JobLogger,
	PermanentFailureEvent
} from './hooks.js'
import type { QueueFailureHandler } from './queue.js'
import { createConsoleLogger } from './default-logger.js'

function report(error: unknown): void {
	try {
		// biome-ignore lint/suspicious/noConsole: Last-resort observer diagnostics must not use the failing custom logger.
		console.error('better-graphile-worker observer failed', error)
	} catch {
		/* Reporting cannot affect job acknowledgement. */
	}
}

export async function observe<T>(
	callback: ((event: T) => void | Promise<void>) | undefined,
	event: T
): Promise<void> {
	try {
		await callback?.(event)
	} catch (error) {
		report(error)
	}
}

export function safeLogger(logger: JobLogger): JobLogger {
	function method(level: keyof JobLogger): JobLogger['info'] {
		return (message, attributes) => {
			try {
				const result: unknown = logger[level](message, attributes)
				if (
					result &&
					typeof (result as PromiseLike<unknown>).then === 'function'
				)
					void Promise.resolve(result).catch(report)
			} catch (error) {
				report(error)
			}
		}
	}
	return {
		debug: method('debug'),
		info: method('info'),
		warn: method('warn'),
		error: method('error')
	}
}

export function jobLogger(
	hooks: BetterWorkerHooks,
	options: CreateLoggerOptions
): JobLogger {
	try {
		return safeLogger(
			hooks.createLogger?.(options) ?? createConsoleLogger(options)
		)
	} catch (error) {
		report(error)
		return safeLogger(createConsoleLogger(options))
	}
}

/** The queue's own hook first, then the instance's; neither can change the outcome. */
export async function reportPermanentFailure(
	hooks: BetterWorkerHooks,
	queueHook: QueueFailureHandler<unknown> | undefined,
	event: PermanentFailureEvent
): Promise<void> {
	if (queueHook)
		await observe((failure) => queueHook(failure.payload, failure), event)
	await observe(hooks.onPermanentFailure, event)
}

/** `hooks.jobAttributes` for one job; a throwing hook is reported and adds nothing. */
export function resolveJobAttributes(
	hooks: BetterWorkerHooks,
	queue: string,
	payload: unknown
): JobAttributes | undefined {
	if (!hooks.jobAttributes) return undefined
	try {
		const attributes = hooks.jobAttributes({ queue, payload })
		return attributes && typeof attributes === 'object' ? attributes : undefined
	} catch (error) {
		report(error)
		return undefined
	}
}
