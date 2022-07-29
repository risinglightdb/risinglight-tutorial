# [RisingLight Tutorial](https://risinglightdb.github.io/risinglight-tutorial)

🚧🚧🚧🚧🚧🚧🚧 UNDER CONSTRUCTION 🚧🚧🚧🚧🚧🚧🚧

[![CI](https://github.com/risinglightdb/risinglight-tutorial/workflows/CI/badge.svg?branch=main)](https://github.com/risinglightdb/risinglight-tutorial/actions)

This repo contains a series of tutorial that help you build an OLAP database system like RisingLight. The tutorial is currently only offered in Simplified Chinese. Let's build an OLAP database from scratch!

**[See it in Action!](https://risinglightdb.github.io/risinglight-tutorial)**

View the [tracking issue](https://github.com/risinglightdb/risinglight-tutorial/issues/1) for the current progress.

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
