{
  "variables": { "openssl_fips": "0" },
  "targets":
    [
      {
        "target_name": "liburing",
        "type": "static_library",
        "include_dirs": ["linux", "liburing/src/include"],
        "direct_dependent_settings":
          { "include_dirs": ["linux", "liburing/src/include"] },
        "sources":
          [
            "liburing/src/queue.c",
            "liburing/src/register.c",
            "liburing/src/setup.c",
            "liburing/src/syscall.c",
          ],
      },
    ],
}
