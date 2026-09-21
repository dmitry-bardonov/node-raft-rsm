# Contributing

Use Node 24 and pnpm 9. Run `pnpm check` before submitting changes. Protocol changes need focused rule tests, a deterministic cluster scenario, and an ADR update when they alter safety/durability semantics. Never fix a failing safety test by adding sleeps, relaxing durability, or deleting the schedule; retain the seed/counterexample.

Keep the core free of wall clock, randomness, filesystem, network, SQLite, and process globals. Ordinary business rejection is typed data; state-machine exceptions are health failures. Update documentation and executable examples with public API changes.

No license or contribution ownership terms have been selected yet, so external contributions should wait for the repository owner to resolve licensing.
