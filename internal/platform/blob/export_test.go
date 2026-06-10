package blob

// WriteFileAtomicForTest exposes the unexported writeFileAtomic to the
// package's external tests. Put always creates the target directory first, so
// the temp-file-creation failure branch is unreachable through the public
// surface; the direct handle lets the test exercise it without weakening
// production visibility.
var WriteFileAtomicForTest = writeFileAtomic
