import { z } from 'zod'
import { Pool } from 'pg'
import * as otel from '@opentelemetry/api'
import {
	createBetterWorker,
	defineQueue,
	defineQueues,
	isRegularQueue,
	isCronQueue,
	isCronInitQueue,
	hasInputSchema,
	type QueueInput,
	type InputsOf,
	type PayloadsOf,
	type TasksOf,
	type QueueContract,
	type UniqueQueueNames
} from 'better-graphile-worker'
import { createJobClient } from 'better-graphile-worker/client'
import { createTestHarness } from 'better-graphile-worker/testing'
import { createCli } from 'better-graphile-worker/cli'

const pool = new Pool()
const contracts = defineQueues([
	defineQueue({ name: 'email', inputSchema: z.object({ to: z.string() }) }),
	defineQueue({
		name: 'count',
		inputSchema: z.string().transform((value) => value.length)
	}),
	defineQueue({ name: 'default', inputSchema: z.string().default('fallback') }),
	defineQueue({ name: 'tick', cron: '* * * * *' }),
	defineQueue({ name: 'gather', cron: '* * * * *', inputSchema: z.number() })
])
const client = createJobClient({
	pgPool: pool,
	queues: contracts,
	otel: { api: otel }
})
client.createJob('email', { to: 'person' })
client.jobs.count('hello')
client.jobs.default()
client.createJob('tick')
client.createJobs('email', [{ to: 'person' }] as const)
client.createJobs('count', ['hello'] as const)
// @ts-expect-error Invalid name
client.createJob('missing', {})
// @ts-expect-error Input required
client.jobs.email()
// @ts-expect-error Producer accepts pre-transform strings
client.createJob('count', 2)
// @ts-expect-error Name and payload must be correlated
client.createJob('email', 'hello')
// @ts-expect-error Batch does not support shared keys
client.createJobs('email', [{ to: 'person' }], { jobKey: 'shared' })
// @ts-expect-error Batch does not support replacement mode
client.createJobs('count', ['hello'], { jobKeyMode: 'replace' })
// @ts-expect-error Cron has no batch inputs
client.createJobs('tick', [undefined])
declare const name: 'email' | 'count'
declare const payload: { to: string } | string
// @ts-expect-error Uncorrelated unions are unsafe
client.createJob(name, payload)

type Equal<A, B> =
	(<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
		? true
		: false
type Assert<T extends true> = T
export type Missing = Assert<
	Equal<QueueInput<'missing', typeof contracts>, never>
>
export type Union = Assert<
	Equal<
		QueueInput<'email' | 'count', typeof contracts>,
		{ to: string } | string
	>
>
export type Input = Assert<Equal<InputsOf<typeof contracts>['count'], string>>
export type Output = Assert<
	Equal<PayloadsOf<typeof contracts>['count'], number>
>
const raw: TasksOf<typeof contracts>['count'] = {
	__bgw: 2,
	payload: 'wire input'
}
void raw
// @ts-expect-error Raw Graphile tasks also use pre-transform input
const invalidRaw: TasksOf<typeof contracts>['count'] = { __bgw: 2, payload: 2 }
void invalidRaw
// @ts-expect-error Duplicates rejected at definition time
defineQueues([
	defineQueue({ name: 'a', inputSchema: z.string() }),
	defineQueue({ name: 'a', inputSchema: z.string() })
])

const worker = createBetterWorker({
	pgPool: pool,
	queues: contracts,
	handlers: {
		email: async (input, ctx) => {
			const queue: 'email' = ctx.queue
			void queue
			await ctx.createJob('count', input.to)
			// @ts-expect-error Context knows the registry
			await ctx.createJob('missing', input)
			// @ts-expect-error Context preserves payload correlation
			await ctx.createJob('email', 1)
			await ctx.step.run('json', () => ({ id: input.to }))
			await ctx.step.run('void', () => undefined)
			// @ts-expect-error Dates require a codec
			await ctx.step.run('date', () => new Date())
			// @ts-expect-error Nested undefined cannot be persisted faithfully
			await ctx.step.run('undefined', () => ({ value: undefined }))
			await ctx.step.run('date-v1', () => new Date(), {
				encode: (date) => date.toISOString(),
				decode: (wire) => new Date(z.string().parse(wire))
			})
		},
		count: async (input, ctx) => {
			const length: number = input
			void length
			await ctx.createJobs('count', ['next'] as const)
		},
		default: async (input) => {
			const value: string = input
			void value
		},
		tick: async (input) => {
			const value: undefined = input
			void value
		},
		gather: {
			initFn: async (ctx) => {
				const name: 'gather_cron-init' = ctx.queue
				void name
				return [1, 2] as const
			},
			processFn: async (input) => {
				const value: number = input
				void value
			}
		}
	}
})
worker.triggerCron('gather')
// @ts-expect-error Trigger restricted to cron names
worker.triggerCron('email')
createCli(worker)
// @ts-expect-error Handler-free contracts require handlers
createBetterWorker({ pgPool: pool, queues: contracts })
createBetterWorker({
	pgPool: pool,
	queues: [contracts[4]],
	// @ts-expect-error Cron-init initializer required
	handlers: { gather: async () => {} }
})

const harness = createTestHarness(contracts, {
	email: async (_input, ctx) => {
		await ctx.createJob('count', 'text')
	},
	count: async () => {},
	default: async () => {},
	tick: async () => {},
	gather: { initFn: async () => [1], processFn: async () => {} }
})
harness.process('count', 'text')
const gathered: Promise<readonly number[]> = harness.init('gather')
void gathered
// @ts-expect-error Only cron-init queues have initializers
harness.init('email')
// @ts-expect-error Process payloads remain correlated
harness.process('email', 1)

function genericProducer<const T extends readonly QueueContract[]>(queues: T) {
	return createJobClient({ pgPool: pool, queues })
}
genericProducer(contracts).jobs.count('text')

// @ts-expect-error Named enqueue functions are immutable
client.jobs.email = async () => null

// Literal registry checks still leave dynamic names to runtime validation.
const reserved = defineQueue({ name: 'then', inputSchema: z.string() })
// @ts-expect-error Promise assimilation reserves "then".
defineQueues([reserved])
// @ts-expect-error Empty names are invalid.
defineQueues([defineQueue({ name: '', inputSchema: z.string() })])
const initTask = defineQueue({
	name: 'gather_cron-init',
	inputSchema: z.string()
})
// @ts-expect-error Generated tasks collide regardless of declaration order.
defineQueues([contracts[4], initTask])
// @ts-expect-error Generated tasks collide regardless of declaration order.
defineQueues([initTask, contracts[4]])
export type ReservedDiagnostic = Assert<
	Equal<
		UniqueQueueNames<readonly [typeof reserved]>,
		'Invalid queue name: "then"'
	>
>
export type CollisionDiagnostic = Assert<
	Equal<
		UniqueQueueNames<readonly [typeof initTask, (typeof contracts)[4]]>,
		'Queue task name collision: gather_cron-init'
	>
>
declare const dynamicName: string
const dynamic = defineQueue({ name: dynamicName, inputSchema: z.string() })
defineQueues([dynamic, contracts[0]])
defineQueues([contracts[0], dynamic])
defineQueues([
	contracts[3],
	defineQueue({ name: 'tick_cron-init', inputSchema: z.string() })
])
// An unknown name must not hide a later known collision.
// @ts-expect-error Known generated-task collisions remain invalid.
defineQueues([dynamic, initTask, contracts[4]])

const syncSchema = z.string().transform((value) => value.length)
const syncContracts = defineQueues([
	defineQueue({ name: 'sync', inputSchema: syncSchema }),
	defineQueue({ name: 'cron', cron: '* * * * *' }),
	defineQueue({ name: 'init', cron: '* * * * *', inputSchema: syncSchema }),
	defineQueue({ name: 'asyncInit', cron: '* * * * *', inputSchema: syncSchema })
])
const syncHandlers = {
	sync: (payload, ctx) => {
		const length: number = payload
		const name: 'sync' = ctx.queue
		void [length, name]
	},
	cron: () => {},
	init: { initFn: () => ['text'] as const, processFn: () => {} },
	asyncInit: {
		initFn: async () => ['text'] as const,
		processFn: async () => {}
	}
} satisfies import('better-graphile-worker').QueueHandlers<typeof syncContracts>
createBetterWorker({
	pgPool: pool,
	queues: syncContracts,
	handlers: syncHandlers
})
const syncHarness = createTestHarness(syncContracts, syncHandlers)
syncHarness.process('sync', 'input')
const initialized: Promise<readonly string[]> = syncHarness.init('init')
void initialized
// @ts-expect-error Handler-free contracts require explicit handlers.
createTestHarness(syncContracts)
const invalidInitializer: import('better-graphile-worker').QueueHandlers<
	typeof syncContracts
> = {
	...syncHandlers,
	// @ts-expect-error Initializers return producer input, before transforms.
	init: { initFn: () => [1] as const, processFn: () => {} }
}
void invalidInitializer

declare const selected:
	| (typeof syncContracts)[0]
	| (typeof syncContracts)[1]
	| (typeof syncContracts)[2]
if (isRegularQueue(selected)) {
	const name: 'sync' = selected.name
	const length: number = selected.inputSchema.parse('text')
	type Exact = Assert<Equal<typeof selected, (typeof syncContracts)[0]>>
	const exact: Exact = true
	void [name, length, exact]
}
if (isCronQueue(selected)) {
	const name: 'cron' = selected.name
	type Exact = Assert<Equal<typeof selected, (typeof syncContracts)[1]>>
	const exact: Exact = true
	void [name, exact]
}
if (isCronInitQueue(selected)) {
	const name: 'init' = selected.name
	const length: number = selected.inputSchema.parse('text')
	type Exact = Assert<Equal<typeof selected, (typeof syncContracts)[2]>>
	const exact: Exact = true
	// @ts-expect-error Contract guards do not invent handler functions.
	selected.initFn
	void [name, length, exact]
}
declare const selectedContract: (typeof contracts)[1] | (typeof contracts)[3]
if (hasInputSchema(selectedContract)) {
	const name: 'count' = selectedContract.name
	const length: number = selectedContract.inputSchema.parse('text')
	type Exact = Assert<Equal<typeof selectedContract, (typeof contracts)[1]>>
	const exact: Exact = true
	void [name, length, exact]
}
// @ts-expect-error Raw Graphile producers must supply the current envelope.
const flatPayload: TasksOf<typeof contracts>['count'] = 'raw'
const oldEnvelope: TasksOf<typeof contracts>['count'] = {
	// @ts-expect-error The previous envelope version is unsupported.
	__bgw: 1,
	payload: 'text'
}
void [flatPayload, oldEnvelope]

import * as publicApi from 'better-graphile-worker'
import * as advanced from 'better-graphile-worker/advanced'
// @ts-expect-error Embedded queue handlers were removed.
publicApi.createQueue
// @ts-expect-error Global tracing mutation was removed.
publicApi.setOtelApi
// @ts-expect-error Internal adapters are available only from /advanced.
publicApi.bindCreateJob
// @ts-expect-error Internal worker builders are available only from /advanced.
publicApi.buildTaskList
// @ts-expect-error Internal SQL adapters are available only from /advanced.
publicApi.createWorkerClient
// @ts-expect-error Stable named functions require an explicit registry.
advanced.createJobsApi(client.createJob)
advanced.bindCreateJob({
	queues: contracts,
	// @ts-expect-error WorkerUtils binding was removed.
	getWorkerUtils: async () => worker.getWorkerUtils()
})
// @ts-expect-error Bounded completion history has an explicit name.
worker.getJobStats().then((rows) => rows[0]?.completed)
worker.getJobStats().then((rows) => rows[0]?.recentCompleted)

// Inline definitions infer handlers without a separate registry.
const inlineEmail = defineQueue({
	name: 'inlineEmail',
	inputSchema: z.object({ to: z.string() }),
	processFn: async (payload, ctx) => {
		const name: 'inlineEmail' = ctx.queue
		const address: string = payload.to
		void [name, address]
		await ctx.createJob(inlineCount, payload.to)
		await ctx.createJobs(inlineCount, ['next'] as const)
		await ctx.createJob(inlineTick)
		await ctx.createJob(inlineOptional)
		// @ts-expect-error Inline enqueue requires a queue reference, not a string.
		await ctx.createJob('inlineCount', payload.to)
		// @ts-expect-error Queue references require producer input before transforms.
		await ctx.createJob(inlineCount, 3)
		// @ts-expect-error Required producer input cannot be omitted.
		await ctx.createJob(inlineCount)
		// @ts-expect-error Batch inputs use producer input before transforms.
		await ctx.createJobs(inlineCount, [3] as const)
		// @ts-expect-error Schema-free cron queues cannot be batched.
		await ctx.createJobs(inlineTick, [undefined])
	}
})
const inlineCount = defineQueue({
	name: 'inlineCount',
	inputSchema: z.string().transform((value) => value.length),
	processFn: async (length, ctx) => {
		const count: number = length
		const name: 'inlineCount' = ctx.queue
		void [count, name]
		// Mutual references must not erase queue names or require annotations.
		await ctx.createJob(inlineEmail, { to: String(length) })
		// @ts-expect-error The mutual target still checks its own input.
		await ctx.createJob(inlineEmail, { to: length })
	}
})
const inlineTick = defineQueue({
	name: 'inlineTick',
	cron: '* * * * *',
	processFn: (payload) => {
		const value: undefined = payload
		void value
	}
})
const inlineOptional = defineQueue({
	name: 'inlineOptional',
	inputSchema: z.string().default('fallback'),
	processFn: (payload) => {
		const value: string = payload
		void value
	}
})
const inlineGather = defineQueue({
	name: 'inlineGather',
	cron: '* * * * *',
	inputSchema: z.string().transform((value) => value.length),
	initFn: (ctx) => {
		const name: 'inlineGather_cron-init' = ctx.queue
		void name
		return ['hello'] as const
	},
	processFn: (length) => {
		const value: number = length
		void value
	}
})
const inlineQueues = defineQueues([
	inlineEmail,
	inlineCount,
	inlineTick,
	inlineOptional,
	inlineGather
])
const inlineWorker = createBetterWorker({ pgPool: pool, queues: inlineQueues })
inlineWorker.createJob('inlineCount', 'hello')
inlineWorker.jobs.inlineEmail({ to: 'hello' })
// @ts-expect-error Instance methods also enforce pre-transform inputs.
inlineWorker.createJob('inlineCount', 3)
const inlineHarness = createTestHarness(inlineQueues)
inlineHarness.process('inlineEmail', { to: 'hello' })
const inlineInputs: Promise<readonly string[]> =
	inlineHarness.init('inlineGather')
void inlineInputs
createBetterWorker({
	pgPool: pool,
	queues: [inlineTick],
	// @ts-expect-error Inline definitions cannot also provide a handler registry.
	handlers: { inlineTick: () => {} }
})
// @ts-expect-error The harness also rejects competing handler registrations.
createTestHarness([inlineTick], { inlineTick: () => {} })
const missingInlineInit = {
	name: 'missingInit',
	cron: '* * * * *',
	inputSchema: z.string(),
	processFn: () => {}
}
// @ts-expect-error Inline cron-init requires an initializer.
defineQueue(missingInlineInit)
const badInlineInit = {
	name: 'badInit',
	cron: '* * * * *',
	inputSchema: z.string().transform((value) => value.length),
	initFn: () => [1],
	processFn: () => {}
}
// @ts-expect-error Inline initFn must return producer input, not transformed output.
defineQueue(badInlineInit)
const badRegularInit = {
	name: 'badRegularInit',
	inputSchema: z.string(),
	initFn: () => ['hello'],
	processFn: () => {}
}
// @ts-expect-error An initializer requires both cron and an input schema.
defineQueue(badRegularInit)
const badInlineHandler = {
	name: 'badHandler',
	inputSchema: z.string(),
	processFn: (payload: number) => {
		void payload
	}
}
// @ts-expect-error Handler annotations cannot widen the schema's payload.
defineQueue(badInlineHandler)

// Queue definitions expose readonly fields; payload types remain ordinary input/output types.
// @ts-expect-error Registered definitions cannot be reassigned accidentally.
inlineEmail.name = 'inlineEmail'
// @ts-expect-error A schema cannot be replaced after defining the queue.
inlineCount.inputSchema = z.string().transform((value) => value.length)
// @ts-expect-error Handler implementations belong to the definition.
inlineEmail.processFn = () => {}
// @ts-expect-error Initializers also belong to the definition.
inlineGather.initFn = () => ['other']
// @ts-expect-error Schedules are readonly definition fields.
inlineTick.cron = '* * * * *'
const configured = defineQueue({
	name: 'configured',
	cron: '* * * * *',
	cronOptions: { priority: 3 },
	processFn: () => {}
})
if (configured.cronOptions) {
	// @ts-expect-error Nested cron defaults cannot be changed accidentally.
	configured.cronOptions.priority = 4
}

// Captured jobs preserve producer input and narrow by the queue discriminator.
for (const job of inlineHarness.enqueued) {
	if (job.queue === 'inlineEmail') {
		const address: string = job.payload.to
		void address
		// @ts-expect-error The email payload has no count field.
		job.payload.count
	}
	if (job.queue === 'inlineCount') {
		const input: string = job.payload
		void input
		// @ts-expect-error Captured values are pre-transform input.
		const output: number = job.payload
		void output
	}
	if (job.queue === 'inlineTick') {
		const input: undefined = job.payload
		void input
	}
}
const exactContext = inlineHarness.context('inlineCount')
const exactQueue: 'inlineCount' = exactContext.queue
void exactQueue
inlineHarness.process('inlineEmail', { to: 'hello' }).then(({ ctx }) => {
	const name: 'inlineEmail' = ctx.queue
	void name
})
// @ts-expect-error Context overrides cannot change the selected queue's identity.
inlineHarness.context('inlineCount', { queue: 'inlineEmail' })
// @ts-expect-error Process overrides also preserve the returned literal queue.
inlineHarness.process('inlineEmail', { to: 'hello' }, { queue: 'inlineCount' })
inlineHarness.init('inlineGather', { queue: 'inlineGather_cron-init' })
// @ts-expect-error Initializer context names use the generated suffix.
inlineHarness.init('inlineGather', { queue: 'inlineGather' })
declare const processArgs:
	| ['inlineEmail', { to: string }]
	| ['inlineCount', string]
inlineHarness.process(...processArgs)
// @ts-expect-error Uncorrelated process arguments remain invalid.
inlineHarness.process(name, payload)

// A reference union is safe only when every possible target accepts the input.
declare const referenceContext: import('better-graphile-worker').InlineJobContext
declare const selectedReference: typeof inlineEmail | typeof inlineCount
declare const selectedInput: { to: string } | string
// @ts-expect-error The selected queue and input are independent unions.
referenceContext.createJob(selectedReference, selectedInput)
// @ts-expect-error A fixed input is also unsafe if one possible queue rejects it.
referenceContext.createJob(selectedReference, 'hello')
// @ts-expect-error Batch inputs must also work for every possible target.
referenceContext.createJobs(selectedReference, ['hello'])
if (selectedReference.name === 'inlineCount') {
	referenceContext.createJob(selectedReference, 'hello')
	referenceContext.createJobs(selectedReference, ['hello'])
}
const otherCount = defineQueue({
	name: 'otherCount',
	inputSchema: z.string(),
	processFn: () => {}
})
declare const compatibleReference: typeof inlineCount | typeof otherCount
referenceContext.createJob(compatibleReference, 'hello')
referenceContext.createJobs(compatibleReference, ['hello'] as const)
const unknownInput = defineQueue({
	name: 'unknownInput',
	inputSchema: z.unknown(),
	processFn: () => {}
})
declare const referenceWithUnknown: typeof unknownInput | typeof inlineEmail
referenceContext.createJob(referenceWithUnknown, { to: 'hello' })
// @ts-expect-error An unknown input must not erase the other target's requirements.
referenceContext.createJob(referenceWithUnknown, 'hello')
// @ts-expect-error The same requirement applies to batch inputs.
referenceContext.createJobs(referenceWithUnknown, ['hello'])
const optionalNumber = defineQueue({
	name: 'optionalNumber',
	inputSchema: z.number().optional(),
	processFn: () => {}
})
declare const optionalReference: typeof inlineOptional | typeof optionalNumber
referenceContext.createJob(optionalReference)
referenceContext.createJob(optionalReference, undefined)
// @ts-expect-error Optional input is safe to omit only if every target allows it.
referenceContext.createJob(selectedReference)
// @ts-expect-error The optional union still rejects an input invalid for one target.
referenceContext.createJob(optionalReference, 'hello')

// Constructor validation also covers definitions built without defineQueue.
const mismatchedDefinition = {
	name: 'mismatched',
	inputSchema: z.string(),
	processFn: (payload: number) => {
		void payload
	}
} as const
const mismatchedOptions = {
	pgPool: pool,
	queues: [mismatchedDefinition] as const
}
// @ts-expect-error A number handler cannot process a string schema.
createBetterWorker(mismatchedOptions)
// @ts-expect-error The harness uses the same schema/handler validation.
createTestHarness([mismatchedDefinition])
const missingInitializer = {
	name: 'missingInitializer',
	cron: '* * * * *',
	inputSchema: z.string(),
	processFn: () => {}
} as const
const missingInitializerOptions = {
	pgPool: pool,
	queues: [missingInitializer] as const
}
// @ts-expect-error Manually constructed cron-init definitions still require initFn.
createBetterWorker(missingInitializerOptions)
// @ts-expect-error The harness also checks missing inline initializers.
createTestHarness([missingInitializer])
const regularWithInitializer = {
	name: 'regularWithInitializer',
	inputSchema: z.string(),
	initFn: () => ['hello'] as const,
	processFn: () => {}
} as const
const regularWithInitializerOptions = {
	pgPool: pool,
	queues: [regularWithInitializer] as const
}
// @ts-expect-error An inline initializer requires a cron schedule.
createBetterWorker(regularWithInitializerOptions)
// @ts-expect-error A regular harness queue cannot have an initializer.
createTestHarness([regularWithInitializer])
const manualDefinition = {
	name: 'manual',
	inputSchema: z.string().transform((value) => value.length),
	processFn: (length: number) => {
		void length
	}
} as const
createBetterWorker({ pgPool: pool, queues: [manualDefinition] })
createTestHarness([manualDefinition]).process('manual', 'hello')

declare const erasedContract: QueueContract
if (hasInputSchema(erasedContract)) {
	// @ts-expect-error Guard narrowing preserves readonly schema fields.
	erasedContract.inputSchema = z.string()
}
