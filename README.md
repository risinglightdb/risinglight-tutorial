# RisingLight Tutorial

[![CI](https://github.com/singularity-data/risinglight-tutorial/workflows/CI/badge.svg?branch=main)](https://github.com/singularity-data/risinglight-tutorial/actions)

Let's build an OLAP database from scratch!

## Building

The documentation is written in [mdBook][mdBook].

[mdBook]: https://github.com/rust-lang/mdBook

To install mdBook:

```sh
cargo install mdbook mdbook-toc
```

Build the documentation:

```sh
cd docs
mdbook build
```

We provide complete codes for each task. 

To build and test these codes:

```sh
cd code
cargo test
```
