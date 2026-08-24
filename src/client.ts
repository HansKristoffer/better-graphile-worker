import { makeWorkerUtils, type WorkerUtils } from 'graphile-worker'
import type { Pool } from 'pg'
import { assertValidSchemaName } from './schema-name'

export const DEFAULT_GRAPHILE_WORKER_SCHEMA = 'graphile_worker'

export type WorkerClient = {
	getUtils(): Promise<WorkerUtils>
	migrate(): Promise<void>
	release(): Promise<void>
}

export function createWorkerClient(options: {
	pgPool: Pool
	schema?: string
}): WorkerClient {
	const schema = assertValidSchemaName(
		options.schema ?? DEFAULT_GRAPHILE_WORKER_SCHEMA
	)
	let utils: WorkerUtils | null = null

	return {
		async getUtils() {
			if (!utils) {
				utils = await makeWorkerUtils({
					pgPool: options.pgPool,
					schema
				})
			}
			return utils
		},
		async migrate() {
			const workerUtils = await this.getUtils()
			await workerUtils.migrate()
		},
		async release() {
			if (utils) {
				await utils.release()
				utils = null
			}
		}
	}
}
