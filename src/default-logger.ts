import type { CreateLoggerOptions, JobLogger, LogAttributes } from './hooks.js'

function formatAttributes(attributes?: LogAttributes): string {
	if (!attributes || Object.keys(attributes).length === 0) return ''
	return ` ${JSON.stringify(attributes)}`
}

export function createConsoleLogger(options: CreateLoggerOptions): JobLogger {
	const prefix = `[${options.queue} ${options.jobId}]`
	const line = (message: string, attributes?: LogAttributes) =>
		`${prefix} ${message}${formatAttributes(
			options.attributes ? { ...options.attributes, ...attributes } : attributes
		)}`
	return {
		debug(message, attributes) {
			console.debug(line(message, attributes))
		},
		info(message, attributes) {
			console.info(line(message, attributes))
		},
		warn(message, attributes) {
			console.warn(line(message, attributes))
		},
		error(message, attributes) {
			console.error(line(message, attributes))
		}
	}
}
