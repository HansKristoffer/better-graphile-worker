import { cp, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

const root = new URL('../', import.meta.url).pathname
const fixture = await mkdtemp(join(tmpdir(), 'bgw-consumer-'))
async function run(args: string[], cwd = fixture) {
	const process = Bun.spawn(args, { cwd, stdout: 'inherit', stderr: 'inherit' })
	const code = await process.exited
	if (code)
		throw new Error(`Consumer check failed (${code}): ${args.join(' ')}`)
}
try {
	await run(
		[
			'npm',
			'pack',
			'--ignore-scripts',
			'--silent',
			'--pack-destination',
			fixture
		],
		root
	)
	const pkg = JSON.parse(await readFile(join(root, 'package.json'), 'utf8'))
	await writeFile(
		join(fixture, 'package.json'),
		JSON.stringify({ private: true, type: 'module' })
	)
	const profile = process.env.PEER_PROFILE ?? 'locked'
	const peerVersions =
		profile === 'minimum'
			? ['graphile-worker@0.17.3', 'zod@4.0.0', 'pg@8.16.0']
			: profile === 'latest'
				? ['graphile-worker@^0.17.3', 'zod@^4', 'pg@^8']
				: Object.entries(pkg.devDependencies)
						.filter(([name]) => ['graphile-worker', 'pg', 'zod'].includes(name))
						.map(([name]) => {
							const resolved = require(
								`${root}/node_modules/${name}/package.json`
							).version
							return `${name}@${resolved}`
						})
	await run([
		'npm',
		'install',
		'--ignore-scripts',
		'--no-package-lock',
		'--no-audit',
		'--no-fund',
		'--legacy-peer-deps',
		`${pkg.name}-${pkg.version}.tgz`,
		...peerVersions,
		'@types/pg@8',
		'@types/node@22',
		`typescript@${process.env.TYPESCRIPT_VERSION ?? '5.9.3'}`
	])
	// Verify every entry point with the optional peer absent, including shared module identity.
	await cp(join(root, 'tests/consumers/smoke.mjs'), join(fixture, 'smoke.mjs'))
	await run([
		...(process.env.NODE_VERSION
			? [
					'npm',
					'exec',
					'--yes',
					'--package',
					`node@${process.env.NODE_VERSION}`,
					'--',
					'node'
				]
			: ['node']),
		'smoke.mjs'
	])
	await run([
		'npm',
		'install',
		'--ignore-scripts',
		'--no-package-lock',
		'--no-audit',
		'--no-fund',
		'--legacy-peer-deps',
		'@opentelemetry/api@1.9.0'
	])
	await cp(join(root, 'tests/consumers/types.ts'), join(fixture, 'types.ts'))
	for (const resolution of ['NodeNext', 'Bundler']) {
		await writeFile(
			join(fixture, 'tsconfig.json'),
			JSON.stringify({
				compilerOptions: {
					target: 'ES2022',
					module: resolution === 'NodeNext' ? 'NodeNext' : 'ESNext',
					moduleResolution: resolution,
					strict: true,
					exactOptionalPropertyTypes: true,
					noUncheckedIndexedAccess: true,
					skipLibCheck: false,
					noEmit: true
				},
				include: ['types.ts']
			})
		)
		await run([
			'node',
			'node_modules/typescript/bin/tsc',
			'-p',
			'tsconfig.json'
		])
	}
	const typescript = JSON.parse(
		await readFile(
			join(fixture, 'node_modules/typescript/package.json'),
			'utf8'
		)
	).version
	console.log(`Packed consumers passed (${profile}, TypeScript ${typescript})`)
} finally {
	await rm(fixture, { recursive: true, force: true })
}
