#!/bin/bash

asc --exportRuntime --runtime stub wasm_test/abi_classes.ts -b wasm_test/abi_classes.wasm || exit $?
asc --exportRuntime --runtime stub wasm_test/abi_store_value.ts -b wasm_test/abi_store_value.wasm || exit $?
asc --exportRuntime --runtime stub wasm_test/abi_token.ts -b wasm_test/abi_token.wasm || exit $?
asc --exportRuntime --runtime stub wasm_test/abort.ts -b wasm_test/abort.wasm || exit $?
asc --exportRuntime --runtime stub wasm_test/big_int_arithmetic.ts -b wasm_test/big_int_arithmetic.wasm || exit $?
asc --exportRuntime --runtime stub wasm_test/big_int_to_hex.ts -b wasm_test/big_int_to_hex.wasm || exit $?
asc --exportRuntime --runtime stub wasm_test/big_int_to_string.ts -b wasm_test/big_int_to_string.wasm || exit $?
asc --exportRuntime --runtime stub wasm_test/bytes_to_base58.ts -b wasm_test/bytes_to_base58.wasm || exit $?
asc --exportRuntime --runtime stub wasm_test/contract_calls.ts -b wasm_test/contract_calls.wasm || exit $?
asc --exportRuntime --runtime stub wasm_test/crypto.ts -b wasm_test/crypto.wasm || exit $?
asc --exportRuntime --runtime stub wasm_test/data_source_create.ts -b wasm_test/data_source_create.wasm || exit $?
asc --exportRuntime --runtime stub wasm_test/ens_name_by_hash.ts -b wasm_test/ens_name_by_hash.wasm || exit $?
asc --exportRuntime --runtime stub wasm_test/ipfs_cat.ts -b wasm_test/ipfs_cat.wasm || exit $?
asc --exportRuntime --runtime stub wasm_test/ipfs_map.ts -b wasm_test/ipfs_map.wasm || exit $?
asc --exportRuntime --runtime stub wasm_test/json_parsing.ts -b wasm_test/json_parsing.wasm || exit $?
asc --exportRuntime --runtime stub wasm_test/non_terminating.ts -b wasm_test/non_terminating.wasm || exit $?
asc --exportRuntime --runtime stub wasm_test/store.ts -b wasm_test/store.wasm || exit $?
asc --exportRuntime --runtime stub wasm_test/string_to_number.ts -b wasm_test/string_to_number.wasm || exit $?
