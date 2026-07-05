package stream

import (
	"github.com/21S1298001/mahiron/internal/stream/databroadcast"
	"github.com/21S1298001/mahiron/internal/stream/local"
	"github.com/21S1298001/mahiron/internal/stream/source"
)

// Aliases re-exporting sub-package types that appear in the public API of
// this package (StreamManagerConfig fields and adapter signatures), so
// external consumers keep importing only internal/stream.
type (
	TunerManager            = source.TunerManager
	TunerAllocator          = source.TunerAllocator
	DecoderCommandProvider  = source.DecoderCommandProvider
	TunerDevice             = source.TunerDevice
	Descrambler             = source.Descrambler
	DescramblerFactory      = source.DescramblerFactory
	EITSectionUpdater       = local.EITSectionUpdater
	LogoUpdater             = local.LogoUpdater
	DataBroadcastEvent      = databroadcast.DataBroadcastEvent
	DataBroadcastModule     = databroadcast.DataBroadcastModule
	DataBroadcastSnapshot   = databroadcast.DataBroadcastSnapshot
	DataBroadcastPMT        = databroadcast.DataBroadcastPMT
	DataBroadcastComponent  = databroadcast.DataBroadcastComponent
	DataBroadcastModuleList = databroadcast.DataBroadcastModuleList
)

func NewCommandDescrambler(command string) Descrambler {
	return source.NewCommandDescrambler(command)
}
