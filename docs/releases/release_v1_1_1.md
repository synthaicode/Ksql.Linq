# Ksql.Linq 1.1.1

This release adds separate Portable PDB distribution through the NuGet symbol package. Each symbol file corresponds to the DLL for the same target framework and release build. Release builds normalize source paths and use Source Link to locate public source at the release commit. The PDB does not embed untracked source files.

Use the NuGet.org symbol server with the DLL from Ksql.Linq 1.1.1 to retrieve its matching Portable PDB.
