# Contributing to thinDB

Thanks for helping. Bug reports, benchmarks, and pull requests are all welcome.

## Before you start

- Read [DESIGN.md](DESIGN.md) before any non-trivial engine change; it explains why thinDB is built the
  way it is. Keep changes thin, explicit, and predictable.
- For a large change, open an issue first so the approach can be agreed before you spend time on it.
- The build and test commands are in the [Development](README.md#development) section of the README.
  `zig build test` and `zig fmt --check src tests bench build.zig scripts` should pass before you open
  a pull request.
- Commit messages are short and in the imperative ("Add dictionary encoder"), with a body explaining
  why when the diff doesn't make it obvious.

## License and Contributor License Agreement

thinDB is licensed under the [Apache License 2.0](LICENSE), and your contributions are published under
it.

Before a pull request can be merged, its author must agree to the
[Contributor License Agreement](CLA.md) once, by including its agreement statement in the pull request
description (the pull request template has it as a checkbox). You keep the copyright in your work. The
agreement lets the maintainer distribute your contribution under the Apache License 2.0 and also under
other terms, such as a commercial license, and it grants the same patent license as the Apache
License.

If you contribute on behalf of your employer, make sure you are allowed to.
