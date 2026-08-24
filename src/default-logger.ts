import type { CreateLoggerOptions, JobLogger, LogAttributes } from './hooks'

function formatAttributes(attributes?: LogAttributes): string {
	if (!attributes || Object.keys(attributes).length === 0) return ''
	return ` ${JSON.stringify(attributes)}`
}

export function createConsoleLogger(options: CreateLoggerOptions): JobLogger {
	const prefix = `[${options.queue} ${options.jobId}]`
	return {
		debug(message, attributes) {
			console.debug(`${prefix} ${message}${formatAttributes(attributes)}`)
		},
		info(message, attributes) {
			console.info(`${prefix} ${message}${formatAttributes(attributes)}`)
		},
		warn(message, attributes) {
			console.warn(`${prefix} ${message}${formatAttributes(attributes)}`)
		},
		error(message, attributes) {
			console.error(`${prefix} ${message}${formatAttributes(attributes)}`)
		}
	}
}
