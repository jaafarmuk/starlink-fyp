# Project Core

This folder isolates the files needed for the current paper-focused project
direction.

Included here:

1. `docs/`
   - target paper PDF
   - `final-decisions.txt`

2. `inputs/`
   - Starlink TLE input used for snapshot generation

3. `tools/`
   - snapshot generation
   - TLE-to-snapshot conversion
   - reliability analysis
   - reliability plotting

4. `results/`
   - snapshot outputs
   - topology stats
   - reliability comparison outputs

This folder intentionally excludes:

- ns-3 C++ simulation files
- packet-level ns-3 outputs
- browser visualizer files

Most files here are symbolic links to the real source files in the repository,
so this folder stays in sync without creating duplicate copies.
