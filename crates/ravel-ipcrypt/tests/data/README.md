# IPCrypt test vectors

`ipcrypt_test_vectors.json` is the `ipcrypt-deterministic` and `ipcrypt-pfx`
subset of the draft's own machine-readable vector file, fetched from
`https://raw.githubusercontent.com/ipcrypt-std/draft-denis-ipcrypt/main/implementations/python/test_vectors.json`
(the `ipcrypt-nd`/`ipcrypt-ndx` entries are dropped: this crate implements
only the two variants ADR-2993 uses). 5 `ipcrypt-deterministic` vectors and
16 `ipcrypt-pfx` vectors, each `{variant, key, ip, encrypted_ip}`.
