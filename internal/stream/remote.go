package stream

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/21S1298001/Mahiron5/internal/config"
	"github.com/21S1298001/Mahiron5/internal/observability"
	"github.com/21S1298001/Mahiron5/internal/program"
	"github.com/21S1298001/Mahiron5/ts"
)

const remoteAvailabilityTimeout = 3 * time.Second

type RemoteClient struct {
	baseURL    string
	basicAuth  *config.BasicAuthConfig
	httpClient *http.Client
}

type ProgramUpdater interface {
	UpsertPrograms(context.Context, []*program.Program) error
}

var newRemoteClient = NewRemoteClient

func NewRemoteClient(config config.RemoteConfig) *RemoteClient {
	return &RemoteClient{
		baseURL:    strings.TrimRight(config.URL, "/"),
		basicAuth:  config.BasicAuth,
		httpClient: http.DefaultClient,
	}
}

func (c *RemoteClient) CheckAvailable(ctx context.Context, channelType string) error {
	return c.CheckAvailableForRoute(ctx, channelType, "")
}

func (c *RemoteClient) CheckAvailableForRoute(ctx context.Context, channelType, channel string) (err error) {
	ctx, span := observability.StartSpan(ctx, observability.SpanRemoteCheckAvailable,
		observability.AttrRemoteURL.String(c.baseURL),
		observability.AttrChannelType.String(channelType),
		observability.AttrChannelID.String(channel),
	)
	defer func() { observability.EndSpan(span, err) }()

	checkCtx, cancel := context.WithTimeout(ctx, remoteAvailabilityTimeout)
	defer cancel()

	req, err := c.newRequest(checkCtx, http.MethodGet, "tuners")
	if err != nil {
		slog.Warn("failed to build remote tuner status request", "remote", c.baseURL, "type", channelType, "err", err)
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		slog.Warn("failed to get remote tuner status", "remote", c.baseURL, "type", channelType, "err", err)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err := fmt.Errorf("remote tuners status: %s", resp.Status)
		slog.Warn("remote tuner status returned non-success", "remote", c.baseURL, "type", channelType, "status", resp.Status)
		return err
	}

	var tuners []remoteTuner
	if err := json.NewDecoder(resp.Body).Decode(&tuners); err != nil {
		slog.Warn("failed to decode remote tuner status", "remote", c.baseURL, "type", channelType, "err", err)
		return err
	}
	for _, tuner := range tuners {
		if !slices.Contains(tuner.Types, channelType) || !tuner.IsAvailable || tuner.IsFault {
			continue
		}
		if tuner.IsFree || tuner.matchesRoute(channelType, channel) {
			return nil
		}
	}
	slog.Debug("remote tuner unavailable", "remote", c.baseURL, "type", channelType)
	return ErrTunerUnavailable
}

func (c *RemoteClient) ChannelStream(ctx context.Context, channelType, channel string, decode bool, dst io.Writer) error {
	return c.stream(ctx, decode, dst, "channels", channelType, channel, "stream")
}

func (c *RemoteClient) ServiceStream(ctx context.Context, channelType, channel string, serviceID uint16, decode bool, dst io.Writer) error {
	return c.stream(ctx, decode, dst, "channels", channelType, channel, "services", fmt.Sprint(serviceID), "stream")
}

func (c *RemoteClient) ProgramStream(ctx context.Context, programID int64, decode bool, dst io.Writer) error {
	return c.stream(ctx, decode, dst, "programs", fmt.Sprint(programID), "stream")
}

func (c *RemoteClient) ScanServices(ctx context.Context, channelType, channel string) (scanned []ts.ServiceInfo, err error) {
	ctx, span := observability.StartSpan(ctx, observability.SpanRemoteScanServices,
		observability.AttrRemoteURL.String(c.baseURL),
		observability.AttrChannelType.String(channelType),
		observability.AttrChannelID.String(channel),
	)
	defer func() { observability.EndSpan(span, err) }()

	var services []remoteService
	if err := c.getJSON(ctx, &services, "channels", channelType, channel, "services"); err != nil {
		return nil, err
	}
	scanned = make([]ts.ServiceInfo, len(services))
	for i, svc := range services {
		scanned[i] = ts.ServiceInfo{
			Nid:                svc.NetworkID,
			Tsid:               svc.TransportStreamID,
			Sid:                svc.ServiceID,
			Name:               svc.Name,
			Type:               uint8(svc.Type),
			LogoId:             int64(svc.LogoID),
			RemoteControlKeyId: uint8Ptr(uint8(svc.RemoteControlKeyID)),
		}
	}
	return scanned, nil
}

func (c *RemoteClient) ListServicePrograms(ctx context.Context, networkID, serviceID uint16) (programs []*program.Program, err error) {
	ctx, span := observability.StartSpan(ctx, observability.SpanRemoteListServicePrograms,
		observability.AttrRemoteURL.String(c.baseURL),
		observability.AttrEPGNetworkID.Int(int(networkID)),
		observability.AttrEPGServiceID.Int(int(serviceID)),
	)
	defer func() { observability.EndSpan(span, err) }()

	req, err := c.newRequest(ctx, http.MethodGet, "programs")
	if err != nil {
		return nil, err
	}
	query := req.URL.Query()
	query.Set("networkId", fmt.Sprint(networkID))
	query.Set("serviceId", fmt.Sprint(serviceID))
	req.URL.RawQuery = query.Encode()

	var remotePrograms []remoteProgram
	if err := c.doJSON(req, &remotePrograms); err != nil {
		return nil, err
	}
	programs = make([]*program.Program, len(remotePrograms))
	for i := range remotePrograms {
		programs[i] = remotePrograms[i].Program()
	}
	return programs, nil
}

func (c *RemoteClient) StreamProgramEvents(ctx context.Context, updater ProgramUpdater) (err error) {
	ctx, span := observability.StartSpan(ctx, observability.SpanRemoteStreamProgramEventsConnect,
		observability.AttrRemoteURL.String(c.baseURL),
	)

	req, err := c.newRequest(ctx, http.MethodGet, "events", "stream")
	if err != nil {
		observability.EndSpan(span, err)
		return err
	}
	query := req.URL.Query()
	query.Set("resource", "program")
	req.URL.RawQuery = query.Encode()

	resp, err := c.httpClient.Do(req)
	if err != nil {
		observability.EndSpan(span, err)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err = fmt.Errorf("remote events stream status: %s", resp.Status)
		observability.EndSpan(span, err)
		return err
	}
	observability.EndSpan(span, nil)
	return readRemoteProgramEvents(ctx, resp.Body, updater)
}

func (c *RemoteClient) stream(ctx context.Context, decode bool, dst io.Writer, elems ...string) error {
	req, err := c.newRequest(ctx, http.MethodGet, elems...)
	if err != nil {
		return err
	}
	if decode {
		query := req.URL.Query()
		query.Set("decode", "1")
		req.URL.RawQuery = query.Encode()
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if resp.StatusCode == http.StatusNotFound {
			return ErrChannelNotFound
		}
		if resp.StatusCode == http.StatusServiceUnavailable {
			return ErrTunerUnavailable
		}
		return fmt.Errorf("remote stream status: %s", resp.Status)
	}
	_, err = io.Copy(dst, resp.Body)
	return err
}

func (c *RemoteClient) getJSON(ctx context.Context, dst any, elems ...string) error {
	req, err := c.newRequest(ctx, http.MethodGet, elems...)
	if err != nil {
		return err
	}
	return c.doJSON(req, dst)
}

func (c *RemoteClient) doJSON(req *http.Request, dst any) error {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("remote API status: %s", resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(dst)
}

func (c *RemoteClient) newRequest(ctx context.Context, method string, elems ...string) (*http.Request, error) {
	u, err := url.Parse(c.baseURL)
	if err != nil {
		return nil, err
	}
	parts := []string{strings.TrimRight(u.Path, "/")}
	for _, elem := range elems {
		parts = append(parts, url.PathEscape(elem))
	}
	u.Path = strings.Join(parts, "/")
	req, err := http.NewRequestWithContext(ctx, method, u.String(), nil)
	if err != nil {
		return nil, err
	}
	if c.basicAuth != nil {
		req.SetBasicAuth(c.basicAuth.Username, c.basicAuth.Password)
	}
	return req, nil
}

type remoteTuner struct {
	Types              []string `json:"types"`
	IsAvailable        bool     `json:"isAvailable"`
	IsFree             bool     `json:"isFree"`
	IsFault            bool     `json:"isFault"`
	CurrentChannelType string   `json:"currentChannelType"`
	CurrentChannel     string   `json:"currentChannel"`
	TunedChannelType   string   `json:"tunedChannelType"`
	TunedChannel       string   `json:"tunedChannel"`
}

func (t remoteTuner) matchesRoute(channelType, channel string) bool {
	if channel == "" {
		return false
	}
	return t.TunedChannelType == channelType && t.TunedChannel == channel ||
		t.CurrentChannelType == channelType && t.CurrentChannel == channel
}

type remoteService struct {
	ServiceID          uint16 `json:"serviceId"`
	NetworkID          uint16 `json:"networkId"`
	TransportStreamID  uint16 `json:"transportStreamId"`
	Name               string `json:"name"`
	Type               int    `json:"type"`
	LogoID             uint64 `json:"logoId"`
	RemoteControlKeyID int    `json:"remoteControlKeyId"`
}

func uint8Ptr(v uint8) *uint8 { return &v }

type remoteProgram struct {
	ID           int64               `json:"id"`
	EventID      uint16              `json:"eventId"`
	ServiceID    uint16              `json:"serviceId"`
	NetworkID    uint16              `json:"networkId"`
	StartAt      int64               `json:"startAt"`
	Duration     int                 `json:"duration"`
	IsFree       bool                `json:"isFree"`
	Name         string              `json:"name"`
	Description  string              `json:"description"`
	Genres       []remoteGenre       `json:"genres"`
	Video        *remoteVideo        `json:"video"`
	Audios       []remoteAudio       `json:"audios"`
	Extended     map[string]string   `json:"extended"`
	RelatedItems []remoteRelatedItem `json:"relatedItems"`
	Series       *remoteSeries       `json:"series"`
}

type remoteEvent struct {
	Resource string          `json:"resource"`
	Type     string          `json:"type"`
	Data     json.RawMessage `json:"data"`
}

func readRemoteProgramEvents(ctx context.Context, src io.Reader, updater ProgramUpdater) error {
	scanner := bufio.NewScanner(src)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 || bytes.Equal(line, []byte("[")) || bytes.Equal(line, []byte(",")) || bytes.Equal(line, []byte("]")) {
			continue
		}
		line = bytes.TrimSuffix(line, []byte(","))
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var event remoteEvent
		if err := json.Unmarshal(line, &event); err != nil {
			slog.Debug("failed to decode remote program event", "err", err)
			continue
		}
		if event.Resource != "program" || event.Type != "update" && event.Type != "create" {
			continue
		}
		var remote remoteProgram
		if err := json.Unmarshal(event.Data, &remote); err != nil {
			slog.Debug("failed to decode remote program event data", "err", err)
			continue
		}
		if err := updater.UpsertPrograms(ctx, []*program.Program{remote.Program()}); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
	return nil
}

func (p remoteProgram) Program() *program.Program {
	prog := &program.Program{
		ID:           p.ID,
		EventID:      p.EventID,
		ServiceID:    p.ServiceID,
		NetworkID:    p.NetworkID,
		StartAt:      p.StartAt,
		Duration:     p.Duration,
		IsFree:       p.IsFree,
		Name:         p.Name,
		Description:  p.Description,
		Genres:       remoteGenres(p.Genres),
		Audios:       remoteAudios(p.Audios),
		Extended:     p.Extended,
		RelatedItems: remoteRelatedItems(p.RelatedItems),
	}
	if p.Video != nil {
		prog.Video = &program.Video{
			StreamContent: p.Video.StreamContent,
			ComponentType: p.Video.ComponentType,
		}
	}
	if p.Series != nil {
		pattern := -1
		if p.Series.Pattern != nil {
			pattern = *p.Series.Pattern
		}
		prog.Series = &program.Series{
			ID:          p.Series.ID,
			Repeat:      p.Series.Repeat,
			Pattern:     pattern,
			ExpiresAt:   p.Series.ExpiresAt,
			Episode:     p.Series.Episode,
			LastEpisode: p.Series.LastEpisode,
			Name:        p.Series.Name,
		}
	}
	return prog
}

type remoteGenre struct {
	Lv1 int `json:"lv1"`
	Lv2 int `json:"lv2"`
	Un1 int `json:"un1"`
	Un2 int `json:"un2"`
}

func remoteGenres(items []remoteGenre) []program.Genre {
	result := make([]program.Genre, len(items))
	for i, item := range items {
		result[i] = program.Genre{Lv1: item.Lv1, Lv2: item.Lv2, Un1: item.Un1, Un2: item.Un2}
	}
	return result
}

type remoteVideo struct {
	StreamContent int `json:"streamContent"`
	ComponentType int `json:"componentType"`
}

type remoteAudio struct {
	ComponentType int      `json:"componentType"`
	ComponentTag  *int     `json:"componentTag"`
	IsMain        *bool    `json:"isMain"`
	SamplingRate  *int     `json:"samplingRate"`
	Langs         []string `json:"langs"`
}

func remoteAudios(items []remoteAudio) []program.Audio {
	result := make([]program.Audio, len(items))
	for i, item := range items {
		result[i] = program.Audio{
			ComponentType: item.ComponentType,
			ComponentTag:  item.ComponentTag,
			IsMain:        item.IsMain,
			SamplingRate:  item.SamplingRate,
			Langs:         item.Langs,
		}
	}
	return result
}

type remoteRelatedItem struct {
	Type      string  `json:"type"`
	NetworkID *uint16 `json:"networkId"`
	ServiceID uint16  `json:"serviceId"`
	EventID   uint16  `json:"eventId"`
}

func remoteRelatedItems(items []remoteRelatedItem) []program.RelatedItem {
	result := make([]program.RelatedItem, len(items))
	for i, item := range items {
		result[i] = program.RelatedItem{
			Type:      program.RelatedItemType(item.Type),
			NetworkID: item.NetworkID,
			ServiceID: item.ServiceID,
			EventID:   item.EventID,
		}
	}
	return result
}

type remoteSeries struct {
	ID          int    `json:"id"`
	Repeat      int    `json:"repeat"`
	Pattern     *int   `json:"pattern"`
	ExpiresAt   *int64 `json:"expiresAt"`
	Episode     int    `json:"episode"`
	LastEpisode int    `json:"lastEpisode"`
	Name        string `json:"name"`
}

type RemoteSessionConfig struct {
	Client       *RemoteClient
	Channel      *config.ChannelConfig
	RouteChannel *config.ChannelConfig
}

type RemoteSession struct {
	channel      *config.ChannelConfig
	client       *RemoteClient
	eventCancel  context.CancelFunc
	eventOnce    sync.Once
	routeChannel *config.ChannelConfig
}

func NewRemoteSession(config RemoteSessionConfig) *RemoteSession {
	return &RemoteSession{
		channel:      config.Channel,
		client:       config.Client,
		routeChannel: config.RouteChannel,
	}
}

func (s *RemoteSession) ChannelStream(ctx context.Context, decode bool, dst io.Writer) error {
	return s.client.ChannelStream(ctx, s.routeChannel.Type, s.routeChannel.Channel, decode, dst)
}

func (s *RemoteSession) ServiceStream(ctx context.Context, serviceID uint16, decode bool, dst io.Writer) error {
	return s.client.ServiceStream(ctx, s.routeChannel.Type, s.routeChannel.Channel, serviceID, decode, dst)
}

func (s *RemoteSession) ProgramStream(ctx context.Context, p *program.Program, decode bool, dst io.Writer) error {
	return s.client.ProgramStream(ctx, p.ID, decode, dst)
}

func (s *RemoteSession) ScanServices(ctx context.Context) ([]ts.ServiceInfo, error) {
	return s.client.ScanServices(ctx, s.routeChannel.Type, s.routeChannel.Channel)
}

func (s *RemoteSession) ListServicePrograms(ctx context.Context, networkID, serviceID uint16) ([]*program.Program, error) {
	return s.client.ListServicePrograms(ctx, networkID, serviceID)
}

func (s *RemoteSession) StartProgramEventSync(updater ProgramUpdater) {
	if updater == nil {
		return
	}
	s.eventOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		s.eventCancel = cancel
		go func() {
			slog.Debug("starting remote program event sync")
			defer slog.Debug("finished remote program event sync")
			if err := s.client.StreamProgramEvents(ctx, updater); err != nil && ctx.Err() == nil {
				slog.Warn("remote program event sync stopped", "err", err)
			}
		}()
	})
}

func (s *RemoteSession) CollectEITS(context.Context, func(*ts.EIT) error) error {
	return ErrEITCollectorNotConfigured
}

func (s *RemoteSession) CollectEITPF(context.Context, func(*ts.EIT) error) error {
	return ErrEITCollectorNotConfigured
}

func (s *RemoteSession) CollectLogos(context.Context, func(*ts.LogoImage) error) error {
	return ErrLogoCollectorNotConfigured
}

func (s *RemoteSession) Stop(context.Context) error {
	if s.eventCancel != nil {
		s.eventCancel()
	}
	return nil
}
