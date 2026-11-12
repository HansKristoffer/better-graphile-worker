# Development

```sh
bun install --frozen-lockfile
bun run lint
DATABASE_URL=postgres://postgres:postgres@localhost:5432/bgw_test bun test
bun run build
bun run test:consumers
bun run attw
bun run publint
bun run verify:package
```

The PostgreSQL tests create/drop their own schema; the existing baseline integration tests also use the default Graphile schema. Use a disposable test database. Packed consumers exercise NodeNext/bundler compilation, optional-peer absence and native Node runtime behavior; set `PEER_PROFILE=minimum|latest`, `TYPESCRIPT_VERSION=5.7.3` and optionally `NODE_VERSION=20` to select a profile.

