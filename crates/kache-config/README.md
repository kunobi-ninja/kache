# kache-config

Resolves Kache configuration from host files, project files and a captured
invocation environment. The client and engine use the same resolver for cache
and runtime paths, trust domains, CI policy and compiler options.

This crate does not open the cache store or start an async runtime.
