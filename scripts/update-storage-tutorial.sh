#!/bin/bash

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

TEMP_DIR=$(mktemp -d "/tmp/risinglight-tutorial.XXXXXX")
CHECKOUT_AT="${TEMP_DIR}/checkout"

cd "$DIR/.."

rm -rf "code/03-00"
rm -rf "code/03-01"
rm -rf "code/03-02"

git worktree prune

git worktree add "${CHECKOUT_AT}" storage --detach
cp -a "${CHECKOUT_AT}/code/03-00/" "code/03-02"
sed -i ".bak" -e "s/risinglight-03-00/risinglight-03-02/g" "code/03-02/Cargo.toml"
sed -i ".bak" -e "s/risinglight_03_00/risinglight_03_02/g" "code/03-02/src/main.rs"
rm "code/03-02/Cargo.toml.bak"
rm "code/03-02/src/main.rs.bak"

cp -a "${CHECKOUT_AT}/code/sql/" "code/sql/"

git worktree remove "${CHECKOUT_AT}"

git worktree add "${CHECKOUT_AT}" storage~1 --detach
cp -a "${CHECKOUT_AT}/code/03-00/" "code/03-01"
sed -i ".bak" -e "s/risinglight-03-00/risinglight-03-01/g" "code/03-01/Cargo.toml"
sed -i ".bak" -e "s/risinglight_03_00/risinglight_03_01/g" "code/03-01/src/main.rs"
rm "code/03-01/Cargo.toml.bak"
rm "code/03-01/src/main.rs.bak"
git worktree remove "${CHECKOUT_AT}"

git worktree add "${CHECKOUT_AT}" storage~2 --detach
cp -a "${CHECKOUT_AT}/code/03-00/" "code/03-00"
git worktree remove "${CHECKOUT_AT}"
