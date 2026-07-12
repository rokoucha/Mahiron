package api

import (
	"context"

	"testing"

	"github.com/21S1298001/mahiron/internal/stream"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

type stubStreamManager struct {
	err error
}

func (s *stubStreamManager) GetOrCreate(context.Context, string, string) (stream.Session, error) {
	return nil, s.err
}

func (s *stubStreamManager) GetExisting(string, string) (stream.Session, bool) {
	return nil, false
}

func (s *stubStreamManager) ActiveSessionCount() int { return 0 }

func TestGetChannelStreamMapsErrTunerUnavailableTo503(t *testing.T) {
	handler, _ := testStreamHeadHandler(t)
	handler.streamManager = &stubStreamManager{err: stream.ErrTunerUnavailable}

	res, err := handler.GetChannelStream(context.Background(), apigen.GetChannelStreamParams{Type: "GR", Channel: "27"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.(*apigen.GetChannelStreamServiceUnavailable); !ok {
		t.Fatalf("response type = %T, want *GetChannelStreamServiceUnavailable", res)
	}
}

func TestGetServiceStreamByChannelMapsErrTunerUnavailableTo503(t *testing.T) {
	handler, _ := testStreamHeadHandler(t)
	handler.streamManager = &stubStreamManager{err: stream.ErrTunerUnavailable}

	res, err := handler.GetServiceStreamByChannel(context.Background(), apigen.GetServiceStreamByChannelParams{Type: "GR", Channel: "27", ID: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.(*apigen.GetServiceStreamByChannelServiceUnavailable); !ok {
		t.Fatalf("response type = %T, want *GetServiceStreamByChannelServiceUnavailable", res)
	}
}

func TestGetServiceStreamMapsErrTunerUnavailableTo503(t *testing.T) {
	handler, sm := testStreamHeadHandler(t)
	svc, err := sm.GetServicesByChannel(context.Background(), "GR", "27")
	if err != nil || len(svc) == 0 {
		t.Fatalf("GetServicesByChannel = %v, %v", svc, err)
	}
	itemID := svc[0].ItemId()
	handler.streamManager = &stubStreamManager{err: stream.ErrTunerUnavailable}

	res, err := handler.GetServiceStream(context.Background(), apigen.GetServiceStreamParams{ID: itemID})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.(*apigen.GetServiceStreamServiceUnavailable); !ok {
		t.Fatalf("response type = %T, want *GetServiceStreamServiceUnavailable", res)
	}
}
