# Extract the cache libraries in reviewable steps

Each extraction targets `main` after its prerequisite has merged. Preserve
cache-key recipes, disk formats, and CLI behavior while moving ownership.
Performance changes follow the structural changes with separate measurements.

1. **Format:** move entry metadata and boundary validators into `kache-format`.
   Keep serialization fixtures and update package, coverage, and release checks.
2. **Store:** move local storage behind storage options and caller-supplied
   artifact rules. Compiler parsing, remote transport, and global configuration
   remain outside its dependency graph. Preserve the existing corruption,
   locking, reclamation, and materialization tests.
3. **Engine:** give local and daemon-assisted requests one implementation of
   lookup, restoration, compilation, and publication. Extract compiler and
   remote adapters where their dependency boundaries are established.
4. **Performance:** reduce repeated input validation and transfer work. Compare
   cold builds, warm builds, changed sources, new worktrees, ephemeral CI, and
   concurrent builds under equivalent conditions.

An extraction is complete when its consumers use the new package and its tests
and release checks follow the code. Empty packages and broad re-exports alone
do not establish a boundary.
