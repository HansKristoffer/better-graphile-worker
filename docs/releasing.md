# Releasing better-graphile-worker

Use a pull request with a conventional title. `fix:` and `perf:` release a patch, `feat:` a minor, and `feat!:` or a `BREAKING CHANGE:` footer a major. `chore:`, `ci:`, `docs:`, `refactor:` and `test:` do not release. Squash merging uses the PR title; do not manually edit versions or changelogs.

CI checks the PR title, lint/types, tests, build and installed npm artifact. The release workflow explicitly dispatches CI on Release Please's branch, so required checks work with the repository's built-in token. No personal access token is needed.

## Publish

1. Merge a passing development PR. Release Please opens or updates a release PR containing the next version and changelog.
2. Review its release notes and any breaking-change migration instructions.
3. Merge the passing release PR when ready. The workflow creates the version tag and GitHub release, checks out that tag, validates the package, and publishes the exact verified tarball with provenance.

Publication uses npm trusted publishing. Its package configuration is GitHub Actions, owner `HansKristoffer`, repository `better-graphile-worker`, workflow `release.yml`, no environment, and permission to publish. Node 24 supplies a compatible npm CLI. Do not add `NPM_TOKEN` or `NODE_AUTH_TOKEN` to this workflow.

## Recover a failed publication

Fix the failure, then run the Release workflow manually with the existing tag:

```sh
gh workflow run release.yml --repo HansKristoffer/better-graphile-worker -f tag=vX.Y.Z
```

The workflow checks that the tag exists as a GitHub release and agrees with the package version. Existing npm versions are skipped; registry errors fail rather than being mistaken for an unpublished version. npm versions are immutable: a broken published package needs a new fix PR and release. A source change cannot repair an old tag's build; cut a new release for that.
