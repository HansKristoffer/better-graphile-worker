export type CompletedJobStatus = 'completed' | 'failed'

export type CompletedJob = {
	id: string
	queueName: string
	payload: unknown
	status: CompletedJobStatus
	attempts: number
	maxAttempts: number
	createdAt: string
	completedAt: string
	durationMs: number
	error: string | null
}

export type CompletedJobStats = Record<
	string,
	{ completed: number; failed: number }
>

const DEFAULT_MAX_JOBS_PER_QUEUE = 50

export type CompletedJobsStore = {
	add(job: CompletedJob): void
	getAll(): CompletedJob[]
	getStats(): CompletedJobStats
}

export function createCompletedJobsStore(
	maxJobsPerQueue = DEFAULT_MAX_JOBS_PER_QUEUE
): CompletedJobsStore {
	const completedJobsMap = new Map<string, CompletedJob[]>()

	return {
		add(job) {
			const queueJobs = completedJobsMap.get(job.queueName) ?? []
			queueJobs.unshift(job)
			if (queueJobs.length > maxJobsPerQueue) {
				queueJobs.pop()
			}
			completedJobsMap.set(job.queueName, queueJobs)
		},
		getAll() {
			const allJobs: CompletedJob[] = []
			for (const jobs of completedJobsMap.values()) {
				allJobs.push(...jobs)
			}
			allJobs.sort(
				(a, b) =>
					new Date(b.completedAt).getTime() - new Date(a.completedAt).getTime()
			)
			return allJobs
		},
		getStats() {
			const stats: CompletedJobStats = {}
			for (const [queueName, jobs] of completedJobsMap.entries()) {
				const completed = jobs.filter((j) => j.status === 'completed').length
				const failed = jobs.filter((j) => j.status === 'failed').length
				stats[queueName] = { completed, failed }
			}
			return stats
		}
	}
}

export function createNoopCompletedJobsStore(): CompletedJobsStore {
	return {
		add() {},
		getAll() {
			return []
		},
		getStats() {
			return {}
		}
	}
}
