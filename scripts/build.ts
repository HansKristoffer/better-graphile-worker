import { mkdir, rm } from 'node:fs/promises'
import { join } from 'node:path'

const root = join(import.meta.dir, '..')
const dist = join(root, 'dist')

type Entry = {
	src: string
	out: string
	wrapBarrel?: boolean
}

const entries: Entry[] = [
	{ src: 'src/index.ts', out: 'index.js', wrapBarrel: true },
	{ src: 'src/cli.ts', out: 'cli.js' },
	{ src: 'src/job-client.ts', out: 'job-client.js' },
	{ src: 'src/testing.ts', out: 'testing.js' }
]

await rm(dist, { recursive: true, force: true })
await mkdir(dist, { recursive: true })

for (const entry of entries) {
	const sourcePath = join(root, entry.src)
	const tmpPath = join(root, '.build-entry.ts')
	const entrypoint = entry.wrapBarrel ? tmpPath : sourcePath

	if (entry.wrapBarrel) {
		// Bun treats a re-export-only file as an empty barrel when this
		// package sets "sideEffects": false. A local binding forces a real bundle.
		await Bun.write(
			tmpPath,
			`export * from ${JSON.stringify(sourcePath)}\nexport const __keep = 1\n`
		)
	}

	const result = await Bun.build({
		entrypoints: [entrypoint],
		outdir: dist,
		naming: entry.out,
		target: 'node',
		format: 'esm',
		packages: 'external'
	})

	if (entry.wrapBarrel) {
		await rm(tmpPath, { force: true })
	}

	if (!result.success) {
		for (const log of result.logs) {
			process.stderr.write(`${String(log)}\n`)
		}
		process.exit(1)
	}
}

const indexPath = join(dist, 'index.js')
const indexJs = await Bun.file(indexPath).text()
await Bun.write(
	indexPath,
	indexJs
		.replace(/\n\/\/ src\/index\.ts\nvar __keep = 1;/, '')
		.replace(/\n\/\/ \.build-entry\.ts\nvar __keep = 1;/, '')
		.replace(/\n {2}__keep,/, '')
)

const tsc = Bun.spawn(['bunx', 'tsc', '-p', 'tsconfig.build.json'], {
	cwd: root,
	stdout: 'inherit',
	stderr: 'inherit'
})
const tscCode = await tsc.exited
if (tscCode !== 0) {
	process.exit(tscCode)
}

function withJsExtension(specifier: string): string {
	if (
		specifier.endsWith('.js') ||
		specifier.endsWith('.json') ||
		specifier.endsWith('.d.ts')
	) {
		return specifier
	}
	return `${specifier}.js`
}

function rewriteDtsSpecifiers(source: string): string {
	return source.replace(
		/(from\s+|import\s*\(\s*)(['"])(\.[^'"]+)\2/g,
		(_match, prefix: string, quote: string, specifier: string) =>
			`${prefix}${quote}${withJsExtension(specifier)}${quote}`
	)
}

for (const file of new Bun.Glob('**/*.d.ts').scanSync({ cwd: dist })) {
	const path = join(dist, file)
	const original = await Bun.file(path).text()
	const rewritten = rewriteDtsSpecifiers(original)
	if (rewritten !== original) {
		await Bun.write(path, rewritten)
	}
}
