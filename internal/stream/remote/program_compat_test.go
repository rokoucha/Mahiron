package remote

import (
	"context"
	"net/http"
	"testing"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/stream/internal/streamtest"
)

func TestDecodeRemoteProgramNullableRelatedFields(t *testing.T) {
	const base = `"id":1,"eventId":1,"serviceId":101,"networkId":4,"startAt":1000,"duration":1000,"isFree":true`
	for _, tc := range []struct {
		name, related string
		want          model.RelatedEvent
	}{
		{"network omitted", `{"type":"shared","serviceId":102,"eventId":2}`, model.RelatedEvent{GroupType: model.EventGroupShared, ServiceID: 102, EventID: 2}},
		{"network null", `{"type":"shared","networkId":null,"serviceId":102,"eventId":2}`, model.RelatedEvent{GroupType: model.EventGroupShared, ServiceID: 102, EventID: 2}},
		{"all null", `{"type":null,"networkId":null,"transportStreamId":null,"serviceId":null,"eventId":null}`, model.RelatedEvent{}},
		{"explicit IDs", `{"type":"relay","networkId":5,"transportStreamId":6,"serviceId":102,"eventId":2}`, model.RelatedEvent{GroupType: model.EventGroupRelay, NetworkID: 5, StreamID: 6, ServiceID: 102, EventID: 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event, err := decodeRemoteProgram([]byte(`{` + base + `,"relatedItems":[` + tc.related + `]}`))
			if err != nil {
				t.Fatal(err)
			}
			if len(event.Related) != 1 || event.Related[0] != tc.want {
				t.Fatalf("related = %+v, want %+v", event.Related, tc.want)
			}
		})
	}
	for _, raw := range []string{
		`{"eventId":1,"relatedItems":[{"networkId":null}]}`,
		`{` + base + `,"relatedItems":[{"networkId":null,"serviceId":"invalid"}]}`,
		`{` + base + `,"relatedItems":[{"networkId":"invalid"}]}`,
		`{` + base + `,"relatedItems":[{"networkId":null}],"duration":null}`,
	} {
		if _, err := decodeRemoteProgram([]byte(raw)); err == nil {
			t.Fatalf("accepted invalid program: %s", raw)
		}
	}
}

func TestListServiceProgramsFiltersUnfilteredRemoteResponse(t *testing.T) {
	client := NewClient(config.RemoteConfig{URL: "http://remote.local/api"})
	client.httpClient = &http.Client{Transport: streamtest.RoundTripFunc(func(r *http.Request) (*http.Response, error) {
		return streamtest.StringResponse(http.StatusOK, `[
   {"id":1,"eventId":1,"networkId":4,"serviceId":101,"startAt":1000,"duration":1000,"isFree":true,"relatedItems":[{"type":"shared","networkId":null,"serviceId":102,"eventId":2}]},
   {"id":2,"eventId":2,"networkId":4,"serviceId":102,"startAt":1000,"duration":1000,"isFree":true},
   {"id":3,"eventId":3,"networkId":5,"serviceId":101,"startAt":1000,"duration":1000,"isFree":true}
  ]`), nil
	})}
	events, err := client.ListServicePrograms(context.Background(), 4, 101)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Key != (model.ServiceKey{NetworkID: 4, ServiceID: 101}) {
		t.Fatalf("programs = %+v", events)
	}
	if len(events[0].Related) != 1 || events[0].Related[0].ServiceID != 102 {
		t.Fatalf("related = %+v", events[0].Related)
	}
}

func TestDecodeRemoteProgramNullVideoResolution(t *testing.T) {
	event, err := decodeRemoteProgram([]byte(`{"id":1,"eventId":1,"serviceId":101,"networkId":4,"startAt":1000,"duration":1000,"isFree":true,"video":{"streamContent":1,"componentType":179,"resolution":null}}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(event.Videos) != 1 || event.Videos[0].Resolution != model.VideoResolution1080i {
		t.Fatalf("videos = %+v", event.Videos)
	}
}
