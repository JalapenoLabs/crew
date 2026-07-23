# Distribution

How the substrate is distributed and consumed. The design decision behind this and
its tradeoff live in `docs/architecture.md` (Distribution); this file is the
operational contract a consumer builds against.

## The decision: a private Git dependency, pinned to a tag

The substrate ships as the `crew-substrate` umbrella crate (issue #34), consumed as a
**private Git dependency** pinned to a release tag (issue #35). Of the three shapes
(private Git dependency, crates.io, a private cargo registry), this is settled for
now because it works today with zero infrastructure, keeps the crate private by
default (the repository is private under **JalapenoLabs**), and is fully reversible:

- **crates.io** is public and wants a stable API before opening; the substrate is
  pre-1.0 and still moving, so it is not appropriate yet. It becomes the plan when
  the API stabilizes at 1.0.
- **A private cargo registry** (Kellnr, Cloudsmith, self-hosted sparse) adds a
  service to run or pay for. It is not justified for one org-internal crate; revisit
  it only if `cargo publish` ergonomics and semver resolution are needed before the
  crate is ready to be public.

The full tradeoff is in `docs/architecture.md`.

## Consuming the substrate

A consumer (Seraphim, or any front-end) depends on the one umbrella crate. In its
`Cargo.toml`:

```toml
[dependencies]
crew-substrate = { git = "ssh://git@github.com/JalapenoLabs/crew.git", tag = "v0.1.0" }
```

- **Pin to a tag** (`tag = "v0.1.0"`) for a stable, human-readable version. Cargo
  records the exact commit in the consumer's `Cargo.lock`, so the build is
  reproducible.
- Only `crew-substrate` is named. Its sibling crates (`crew-core`, `crew-broker`,
  `crew-supervisor`, `crew-mcp`) are path dependencies inside the repository, so
  cargo resolves them from the same checkout automatically; a consumer never lists
  them.
- To track an unreleased fix, pin a commit instead: `rev = "<sha>"`. Pinning a
  `branch` is possible but not reproducible, so prefer a `tag` or `rev`.
- Over HTTPS instead of SSH (useful in CI with a token):
  `git = "https://github.com/JalapenoLabs/crew.git"`.

Because the repository is private, the consumer's environment needs read access to
fetch it: a developer's SSH key, or in CI a deploy key or a token with `repo` read
scope. `cargo build` then resolves and pins the crate like any other dependency.

`publish = false` on every crate is intentional and does **not** block a Git
dependency (that flag only blocks `cargo publish` to a registry). It guards against
an accidental public crates.io publish while the crate is private.

## Versioning

The workspace carries a single version in `Cargo.toml` (`[workspace.package]
version`), inherited by every crate, so they never drift. It follows semantic
versioning; while it is **pre-1.0**, a minor bump may change the public API. A
consumer pins a tag and upgrades deliberately.

## Cutting a release

A release is a Git tag `v<version>` on `main`. To cut one:

1. Bump `version` in the workspace `Cargo.toml`, and run `cargo build` so
   `Cargo.lock` records it. Land it on `main`.
2. Tag the merge commit and push the tag:

   ```sh
   git tag v0.1.0
   git push origin v0.1.0
   ```

3. The release workflow (`.github/workflows/release.yml`) runs on the `v*` tag:
   it builds the tagged commit and, if it builds, creates a GitHub Release for the
   tag. Consumers then pin `tag = "v0.1.0"`.

Tag only commits on `main` that have passed CI, so a pinned version is always a
green build.
