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
  `crew-client`, `crew-supervisor`, `crew-mcp`) are path dependencies inside the repository, so
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

## Semantic versioning

The workspace carries a single version in `Cargo.toml` (`[workspace.package]
version`), inherited by every crate, so they never drift. A consumer pins one tag,
`v<version>`, and the whole substrate moves together.

**The public API** is the surface `crew-substrate` re-exports (issue #34): the items a
consumer can name through the umbrella crate. Everything else, the internal crates'
private items and the CLI, is not part of the contract and may change freely.

The version follows [semantic versioning](https://semver.org). A change to the public
API sets the bump:

- **Patch** (`0.1.0` to `0.1.1`): a backward-compatible fix. No change a consumer must
  react to.
- **Minor** (`0.1.0` to `0.2.0`): a backward-compatible addition, such as a new
  exported item or a new optional argument.
- **Major** (`0.1.0` to `1.0.0`): a breaking change, such as a removed or renamed item,
  a changed signature, or a behavior a consumer relies on.

While the substrate is **pre-1.0**, the `0.x` line shifts these one place left, per the
semver spec: a **minor** bump (`0.1` to `0.2`) may break the API, and only a **patch**
bump is guaranteed compatible. Pin a tag and upgrade deliberately; read the release
notes (below) before bumping across a minor version.

The version stabilizes at `1.0` when the API is settled, which is also when crates.io
becomes the plan (see the decision above).

## Changelog

The changelog for a version is its **GitHub Release notes**, generated from the pull
requests merged since the previous tag (`gh release create --generate-notes`, run by
`release.yml`). Each tag gets one set of notes, a point-in-time record of what that
version contains.

The repository keeps **no `CHANGELOG.md`**. A hand-maintained changelog narrates
history ("moved from X to Y", "added A, deferred B"), which this project's
documentation policy excludes: the docs describe the current design and the roadmap,
not the path taken to them (see `CLAUDE.md`, History). The per-tag release notes carry
the same information a consumer needs, what changed between two versions, without a
maintained history file to keep in sync.

## Cutting a release

A release is a Git tag `v<version>` on `main`. To cut one:

1. Bump `version` in the workspace `Cargo.toml`, and run `cargo build` so
   `Cargo.lock` records it. Land it on `main`.
2. Tag the merge commit and push the tag:

   ```sh
   git tag v0.1.0
   git push origin v0.1.0
   ```

3. The release workflow (`.github/workflows/release.yml`) runs on the `v*` tag. On
   the pinned toolchain it: verifies the tag names the crate's own version (so a tag
   never publishes a mismatched version), runs the full gate (fmt, clippy, tests) to
   prove the tagged commit is green, then creates a GitHub Release with generated
   notes. Consumers then pin `tag = "v0.1.0"`.

The version guard means step 1 is mandatory: bump `[workspace.package] version` to
match the tag, or the release fails before publishing. Tag only commits on `main` that
have passed CI, so a pinned version is always a green build.
