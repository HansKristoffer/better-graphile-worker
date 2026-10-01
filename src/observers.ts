import type {
	BetterWorkerHooks,
	CreateLoggerOptions,
	JobLogger
} from './hooks.js'
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
