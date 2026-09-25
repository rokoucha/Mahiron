// Package mirakurun gathers the bidirectional conversions between Mahiron's
// domain objects and the Mirakurun-compatible JSON shapes: the ogen API
// types, the /api/events payloads and the data received from remote
// Mirakurun-compatible servers.
//
// The conversions currently work on program.Program and service.Service.
// Once the internal broadcast model (docs/isdb-s3.md phase 3-5/3-6) lands,
// this package will only depend on that model and the ogen types; the
// program, service and ts imports below are temporary scaffolding for that
// move, not a layer to build on.
package mirakurun
