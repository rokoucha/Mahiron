package api

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/internal/epg"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/service"
	"github.com/21S1298001/mahiron/internal/stream"
	"github.com/21S1298001/mahiron/internal/stream/databroadcast"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

func testProgramHandler(t *testing.T) *Handler {
	t.Helper()
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	pm := program.NewProgramManager(program.NewSQLiteStore(database))
	updater := epg.NewUpdater(pm)
	if err := updater.UpsertEITSection(ctx, &epg.EITSection{
		OriginalNetworkID: 1,
		ServiceID:         101,
		Events: []epg.EITEvent{
			{EventID: 10, StartTime: 2000, Duration: 30000, Scrambled: false,
				Descriptors: []epg.EITDescriptor{
					{Type: "ShortEvent", EventName: "second"},
				},
			},
			{EventID: 9, StartTime: 1000, Duration: 30000, Scrambled: false,
				Descriptors: []epg.EITDescriptor{
					{Type: "ShortEvent", EventName: "first"},
				},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	serviceStore := service.NewSQLiteStore(database)
	if err := serviceStore.ReplaceChannelServices(ctx, "GR", "27", []*service.Service{
		{Id: "0000100101", ServiceId: 101, NetworkId: 1, Name: "NHK Service", ChannelType: "GR", ChannelId: "27"},
	}); err != nil {
		t.Fatal(err)
	}
	return NewHandler(HandlerConfig{
		ProgramManager: pm,
		ServiceManager: service.NewServiceManager(serviceStore, config.ChannelsConfig{
			{Name: "NHK", Type: "GR", Channel: "27"},
		}),
	})
}

func TestGetProgramsFiltersAndSorts(t *testing.T) {
	handler := testProgramHandler(t)

	res, err := handler.GetPrograms(context.Background(), apigen.GetProgramsParams{
		ServiceId: apigen.NewOptInt(101),
	})
	if err != nil {
		t.Fatal(err)
	}
	programs, ok := res.(*apigen.GetProgramsOKApplicationJSON)
	if !ok {
		t.Fatalf("response type = %T, want *GetProgramsOKApplicationJSON", res)
	}
	if got, want := len(*programs), 2; got != want {
		t.Fatalf("programs length = %d, want %d", got, want)
	}
	if got, want := (*programs)[0].Name.Value, "first"; got != want {
		t.Fatalf("first program name = %q, want %q", got, want)
	}
}

func TestGetProgramReturnsProgramAndNotFound(t *testing.T) {
	handler := testProgramHandler(t)

	res, err := handler.GetProgram(context.Background(), apigen.GetProgramParams{ID: program.ProgramID(1, 101, 9)})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.(*apigen.Program); !ok {
		t.Fatalf("response type = %T, want *Program", res)
	}

	res, err = handler.GetProgram(context.Background(), apigen.GetProgramParams{ID: 999})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.(*apigen.ErrorStatusCode); !ok {
		t.Fatalf("response type = %T, want *ErrorStatusCode", res)
	}
}

func TestGetServicePrograms(t *testing.T) {
	handler := testProgramHandler(t)

	res, err := handler.GetServicePrograms(context.Background(), apigen.GetServiceProgramsParams{ID: 100101})
	if err != nil {
		t.Fatal(err)
	}
	programs, ok := res.(*apigen.GetServiceProgramsOKApplicationJSON)
	if !ok {
		t.Fatalf("response type = %T, want *GetServiceProgramsOKApplicationJSON", res)
	}
	if got, want := len(*programs), 2; got != want {
		t.Fatalf("programs length = %d, want %d", got, want)
	}
}

func TestGetProgramStreamReturnsStreamAndTunerUserID(t *testing.T) {
	handler := testProgramHandler(t)
	handler.streamManager = fakeProgramStreamManager{session: fakeProgramStreamSession{data: "program-ts"}}

	res, err := handler.GetProgramStream(context.Background(), apigen.GetProgramStreamParams{
		ID: program.ProgramID(1, 101, 9),
	})
	if err != nil {
		t.Fatal(err)
	}
	ok, isOK := res.(*apigen.GetProgramStreamOKHeaders)
	if !isOK {
		t.Fatalf("response type = %T, want *GetProgramStreamOKHeaders", res)
	}
	userID, set := ok.XMirakurunTunerUserID.Get()
	if !set || userID == "" {
		t.Fatal("X-Mirakurun-Tuner-User-ID should be set")
	}
	body, err := io.ReadAll(ok.Response.Data)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "program-ts" {
		t.Fatalf("stream body = %q, want program-ts", body)
	}
}

func TestGetProgramStreamMissingProgramAndService(t *testing.T) {
	handler := testProgramHandler(t)
	handler.streamManager = fakeProgramStreamManager{session: fakeProgramStreamSession{}}

	res, err := handler.GetProgramStream(context.Background(), apigen.GetProgramStreamParams{ID: 999})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.(*apigen.GetProgramStreamNotFound); !ok {
		t.Fatalf("response type = %T, want *GetProgramStreamNotFound", res)
	}

	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	pm := program.NewProgramManager(program.NewSQLiteStore(database))
	if err := pm.ReplaceServicePrograms(ctx, 1, 101, 0, []*program.Program{
		{ID: program.ProgramID(1, 101, 9), NetworkID: 1, ServiceID: 101, EventID: 9, StartAt: 1000, Duration: 1000},
	}); err != nil {
		t.Fatal(err)
	}
	missingServiceHandler := NewHandler(HandlerConfig{
		ProgramManager: pm,
		ServiceManager: service.NewServiceManager(service.NewSQLiteStore(database), config.ChannelsConfig{}),
		StreamManager:  fakeProgramStreamManager{session: fakeProgramStreamSession{}},
	})
	res, err = missingServiceHandler.GetProgramStream(context.Background(), apigen.GetProgramStreamParams{ID: program.ProgramID(1, 101, 9)})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.(*apigen.GetProgramStreamNotFound); !ok {
		t.Fatalf("response type = %T, want *GetProgramStreamNotFound", res)
	}
}

func TestGetProgramStreamTunerUnavailable(t *testing.T) {
	handler := testProgramHandler(t)
	handler.streamManager = fakeProgramStreamManager{err: stream.ErrTunerUnavailable}

	res, err := handler.GetProgramStream(context.Background(), apigen.GetProgramStreamParams{ID: program.ProgramID(1, 101, 9)})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.(*apigen.GetProgramStreamServiceUnavailable); !ok {
		t.Fatalf("response type = %T, want *GetProgramStreamServiceUnavailable", res)
	}
}

func TestProgramsIDStreamHeadOnlyRequiresProgram(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	pm := program.NewProgramManager(program.NewSQLiteStore(database))
	id := program.ProgramID(1, 101, 9)
	if err := pm.ReplaceServicePrograms(ctx, 1, 101, 0, []*program.Program{
		{ID: id, NetworkID: 1, ServiceID: 101, EventID: 9, StartAt: 1000, Duration: 1000},
	}); err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(HandlerConfig{
		ProgramManager: pm,
		ServiceManager: service.NewServiceManager(service.NewSQLiteStore(database), config.ChannelsConfig{}),
	})
	res, err := handler.ProgramsIDStreamHead(context.Background(), apigen.ProgramsIDStreamHeadParams{ID: id})
	if err != nil {
		t.Fatal(err)
	}
	ok, isOK := res.(*apigen.ProgramsIDStreamHeadOK)
	if !isOK {
		t.Fatalf("response type = %T, want *ProgramsIDStreamHeadOK", res)
	}
	userID, set := ok.XMirakurunTunerUserID.Get()
	if !set || userID == "" {
		t.Fatal("X-Mirakurun-Tuner-User-ID should be set")
	}
}

func TestApiProgramExposesExtendedRelatedAndSeries(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	pm := program.NewProgramManager(program.NewSQLiteStore(database))
	id := program.ProgramID(1, 101, 7)
	nid := uint16(1)
	tsid := uint16(10)
	if err := pm.ReplaceServicePrograms(ctx, 1, 101, 0, []*program.Program{
		{
			ID:        id,
			NetworkID: 1,
			ServiceID: 101,
			EventID:   7,
			StartAt:   1000,
			Duration:  1000,
			Extended:  map[string]string{"出演者": "Foo"},
			RelatedItems: []program.RelatedItem{
				{Type: program.RelatedItemTypeShared, NetworkID: &nid, TransportStreamID: &tsid, ServiceID: 101, EventID: 9},
			},
			Series: &program.Series{ID: 5, Pattern: 1, Episode: 1, LastEpisode: 12, Name: "series"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(HandlerConfig{ProgramManager: pm})
	res, err := handler.GetProgram(context.Background(), apigen.GetProgramParams{ID: id})
	if err != nil {
		t.Fatal(err)
	}
	p, ok := res.(*apigen.Program)
	if !ok {
		t.Fatalf("response type = %T, want *Program", res)
	}
	if !p.Extended.IsSet() {
		t.Fatal("Extended not set")
	}
	if p.Extended.Value["出演者"] != "Foo" {
		t.Errorf("Extended[出演者] = %q, want Foo", p.Extended.Value["出演者"])
	}
	if len(p.RelatedItems) != 1 {
		t.Fatalf("RelatedItems = %d, want 1", len(p.RelatedItems))
	}
	if p.RelatedItems[0].Type.Value != apigen.RelatedItemTypeShared {
		t.Errorf("RelatedItem.Type = %v, want shared", p.RelatedItems[0].Type.Value)
	}
	if p.RelatedItems[0].TransportStreamId.Value != 10 {
		t.Errorf("RelatedItem.TransportStreamId = %d, want 10", p.RelatedItems[0].TransportStreamId.Value)
	}
	if !p.Series.IsSet() {
		t.Fatal("Series not set")
	}
	if p.Series.Value.ID.Value != 5 {
		t.Errorf("Series.ID = %d, want 5", p.Series.Value.ID.Value)
	}
}

func TestApiProgramVideoTypeAndResolution(t *testing.T) {
	tests := []struct {
		name           string
		video          *program.Video
		wantType       apigen.ProgramVideoType
		wantTypeSet    bool
		wantResolution apigen.ProgramVideoResolution
		wantResSet     bool
	}{
		{
			name:           "mpeg2 1080i",
			video:          &program.Video{StreamContent: 0x1, ComponentType: 0xB3},
			wantType:       apigen.ProgramVideoTypeMpeg2,
			wantTypeSet:    true,
			wantResolution: apigen.ProgramVideoResolution1080i,
			wantResSet:     true,
		},
		{
			name:           "h264 720p",
			video:          &program.Video{StreamContent: 0x5, ComponentType: 0xC3},
			wantType:       apigen.ProgramVideoTypeH264,
			wantTypeSet:    true,
			wantResolution: apigen.ProgramVideoResolution720p,
			wantResSet:     true,
		},
		{
			name:           "h265 4320p",
			video:          &program.Video{StreamContent: 0x9, ComponentType: 0x83},
			wantType:       apigen.ProgramVideoTypeH265,
			wantTypeSet:    true,
			wantResolution: apigen.ProgramVideoResolution4320p,
			wantResSet:     true,
		},
		{
			name:        "unknown values keep raw fields only",
			video:       &program.Video{StreamContent: 0xF, ComponentType: 0xF1},
			wantTypeSet: false,
			wantResSet:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := apiProgram(&program.Program{Video: tt.video})
			video, ok := p.Video.Get()
			if !ok {
				t.Fatal("Video not set")
			}
			if got, ok := video.StreamContent.Get(); !ok || got != tt.video.StreamContent {
				t.Fatalf("StreamContent = %d, %v; want %d, true", got, ok, tt.video.StreamContent)
			}
			if got, ok := video.ComponentType.Get(); !ok || got != tt.video.ComponentType {
				t.Fatalf("ComponentType = %d, %v; want %d, true", got, ok, tt.video.ComponentType)
			}
			gotType, gotTypeSet := video.Type.Get()
			if gotTypeSet != tt.wantTypeSet || gotType != tt.wantType {
				t.Errorf("Type = %q, %v; want %q, %v", gotType, gotTypeSet, tt.wantType, tt.wantTypeSet)
			}
			gotRes, gotResSet := video.Resolution.Get()
			if gotResSet != tt.wantResSet || gotRes != tt.wantResolution {
				t.Errorf("Resolution = %q, %v; want %q, %v", gotRes, gotResSet, tt.wantResolution, tt.wantResSet)
			}
		})
	}
}

type fakeProgramStreamManager struct {
	err     error
	session fakeProgramStreamSession
}

func (m fakeProgramStreamManager) GetOrCreate(context.Context, string, string) (stream.Session, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.session, nil
}

func (m fakeProgramStreamManager) GetExisting(string, string) (stream.Session, bool) {
	return m.session, m.err == nil
}

func (m fakeProgramStreamManager) ActiveSessionCount() int {
	return 0
}

type fakeProgramStreamSession struct {
	stream.Session
	data string
	err  error
}

func (s fakeProgramStreamSession) ChannelStream(context.Context, bool, io.Writer) error {
	return errors.New("unexpected ChannelStream call")
}

func (s fakeProgramStreamSession) ServiceStream(context.Context, uint16, bool, io.Writer) error {
	return errors.New("unexpected ServiceStream call")
}

func (s fakeProgramStreamSession) ProgramStream(_ context.Context, _ *program.Program, _ bool, dst io.Writer) error {
	if s.err != nil {
		return s.err
	}
	_, err := io.WriteString(dst, s.data)
	return err
}

func (s fakeProgramStreamSession) ObserveDataBroadcast(context.Context, uint16, bool, func(databroadcast.DataBroadcastEvent) error) error {
	return errors.New("unexpected ObserveDataBroadcast call")
}

func (s fakeProgramStreamSession) DataBroadcastModule(uint16, byte, uint16) (databroadcast.DataBroadcastModule, bool) {
	return databroadcast.DataBroadcastModule{}, false
}

func TestApiProgramRelatedItemsEmptyWhenNone(t *testing.T) {
	handler := testProgramHandler(t)
	res, err := handler.GetProgram(context.Background(), apigen.GetProgramParams{ID: program.ProgramID(1, 101, 9)})
	if err != nil {
		t.Fatal(err)
	}
	p := res.(*apigen.Program)
	if p.RelatedItems == nil {
		t.Fatal("RelatedItems should be a non-nil empty slice")
	}
	if len(p.RelatedItems) != 0 {
		t.Errorf("RelatedItems = %d, want 0", len(p.RelatedItems))
	}
	if p.Extended.IsSet() {
		t.Errorf("Extended = %#v, want unset", p.Extended)
	}
	if p.Series.IsSet() {
		t.Errorf("Series = %#v, want unset", p.Series)
	}
}
