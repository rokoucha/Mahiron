package api

import (
	"github.com/go-faster/jx"

	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

// ogen generates JSON encoders only for types reachable from a JSON
// response, and the event stream is a raw text/event-stream response. The
// event envelope and the payloads only the stream carries (moduleList and
// esEvent) are therefore encoded here, following api.yml; every payload the
// state endpoint shares uses its generated encoder.

func encodeDataBroadcastEvent(e *jx.Encoder, event apigen.DataBroadcastEvent) {
	e.ObjStart()
	e.FieldStart("type")
	e.Str(event.Type)
	e.FieldStart("sequence")
	e.Int64(event.Sequence)
	e.FieldStart("revision")
	e.Int64(event.Revision)
	if value, ok := event.Snapshot.Get(); ok {
		e.FieldStart("snapshot")
		value.Encode(e)
	}
	encodeOptNil(e, "pmt", event.Pmt.Set, event.Pmt.Null, event.Pmt.Value.Encode)
	encodeOptNil(e, "moduleList", event.ModuleList.Set, event.ModuleList.Null, func(e *jx.Encoder) {
		encodeDataBroadcastModuleList(e, event.ModuleList.Value)
	})
	encodeOptNil(e, "module", event.Module.Set, event.Module.Null, event.Module.Value.Encode)
	encodeOptNil(e, "programInfo", event.ProgramInfo.Set, event.ProgramInfo.Null, event.ProgramInfo.Value.Encode)
	encodeOptNil(e, "currentTime", event.CurrentTime.Set, event.CurrentTime.Null, event.CurrentTime.Value.Encode)
	encodeOptNil(e, "esEvent", event.EsEvent.Set, event.EsEvent.Null, func(e *jx.Encoder) {
		encodeDataBroadcastESEvent(e, event.EsEvent.Value)
	})
	encodeOptNil(e, "bit", event.Bit.Set, event.Bit.Null, event.Bit.Value.Encode)
	encodeOptNil(e, "pcr", event.Pcr.Set, event.Pcr.Null, event.Pcr.Value.Encode)
	e.ObjEnd()
}

// encodeOptNil writes an optional nullable field: absent when unset, null
// when null.
func encodeOptNil(e *jx.Encoder, name string, set, null bool, encode func(*jx.Encoder)) {
	if !set {
		return
	}
	e.FieldStart(name)
	if null {
		e.Null()
		return
	}
	encode(e)
}

func encodeDataBroadcastModuleList(e *jx.Encoder, list apigen.DataBroadcastModuleList) {
	e.ObjStart()
	e.FieldStart("componentTag")
	e.Int(list.ComponentTag)
	e.FieldStart("downloadId")
	e.Int64(list.DownloadId)
	e.FieldStart("blockSize")
	e.Int(list.BlockSize)
	e.FieldStart("dataEventId")
	e.Int(list.DataEventId)
	e.FieldStart("returnToEntry")
	if value, ok := list.ReturnToEntry.Get(); ok {
		e.Bool(value)
	} else {
		e.Null()
	}
	e.FieldStart("modules")
	e.ArrStart()
	for _, module := range list.Modules {
		module.Encode(e)
	}
	e.ArrEnd()
	e.ObjEnd()
}

func encodeDataBroadcastESEvent(e *jx.Encoder, event apigen.DataBroadcastESEvent) {
	e.ObjStart()
	e.FieldStart("componentId")
	e.Int(event.ComponentId)
	e.FieldStart("dataEventId")
	e.Int(event.DataEventId)
	e.FieldStart("events")
	e.ArrStart()
	for _, item := range event.Events {
		encodeDataBroadcastGeneralEvent(e, item)
	}
	e.ArrEnd()
	e.ObjEnd()
}

func encodeDataBroadcastGeneralEvent(e *jx.Encoder, event apigen.DataBroadcastGeneralEvent) {
	e.ObjStart()
	e.FieldStart("type")
	e.Str(event.Type)
	if value, ok := event.PostDiscontinuityIndicator.Get(); ok {
		e.FieldStart("postDiscontinuityIndicator")
		e.Bool(value)
	}
	encodeOptInt(e, "dsmContentId", event.DsmContentId)
	encodeOptInt64(e, "STCReference", event.STCReference)
	encodeOptInt64(e, "NPTReference", event.NPTReference)
	encodeOptInt(e, "scaleNumerator", event.ScaleNumerator)
	encodeOptInt(e, "scaleDenominator", event.ScaleDenominator)
	encodeOptInt(e, "eventMessageGroupId", event.EventMessageGroupId)
	encodeOptInt(e, "timeMode", event.TimeMode)
	encodeOptInt(e, "eventMessageType", event.EventMessageType)
	encodeOptInt(e, "eventMessageId", event.EventMessageId)
	if event.PrivateDataByte != nil {
		e.FieldStart("privateDataByte")
		e.ArrStart()
		for _, value := range event.PrivateDataByte {
			e.Int(value)
		}
		e.ArrEnd()
	}
	encodeOptInt64(e, "eventMessageNPT", event.EventMessageNPT)
	e.ObjEnd()
}

func encodeOptInt(e *jx.Encoder, name string, value apigen.OptInt) {
	if v, ok := value.Get(); ok {
		e.FieldStart(name)
		e.Int(v)
	}
}

func encodeOptInt64(e *jx.Encoder, name string, value apigen.OptInt64) {
	if v, ok := value.Get(); ok {
		e.FieldStart(name)
		e.Int64(v)
	}
}
