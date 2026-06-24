package stream

import (
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
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/ts"
)

const remoteAvailabilityTimeout = 3 * time.Second

const (
	remoteOperationCheckAvailable      = "remote.check_available"
	remoteOperationChannelStream       = "remote.channel_stream"
	remoteOperationServiceStream       = "remote.service_stream"
	remoteOperationProgramStream       = "remote.program_stream"
	remoteOperationScanServices        = "remote.scan_services"
	remoteOperationListServicePrograms = "remote.list_service_programs"
	remoteOperationStreamProgramEvents = "remote.stream_program_events"
)

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
	start := time.Now()
	defer func() {
		observability.RecordRemoteOperation(ctx, remoteOperationCheckAvailable, remoteOperationResult(err), time.Since(start).Milliseconds())
	}()

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
	return c.stream(ctx, remoteOperationChannelStream, decode, dst, "channels", channelType, channel, "stream")
}

func (c *RemoteClient) ServiceStream(ctx context.Context, channelType, channel string, serviceID uint16, decode bool, dst io.Writer) error {
	return c.stream(ctx, remoteOperationServiceStream, decode, dst, "channels", channelType, channel, "services", fmt.Sprint(serviceID), "stream")
}

func (c *RemoteClient) ProgramStream(ctx context.Context, programID int64, decode bool, dst io.Writer) error {
	return c.stream(ctx, remoteOperationProgramStream, decode, dst, "programs", fmt.Sprint(programID), "stream")
}

func (c *RemoteClient) ScanServices(ctx context.Context, channelType, channel string) (scanned []ts.ServiceInfo, err error) {
	start := time.Now()
	defer func() {
		observability.RecordRemoteOperation(ctx, remoteOperationScanServices, remoteOperationResult(err), time.Since(start).Milliseconds())
	}()

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
	start := time.Now()
	defer func() {
		observability.RecordRemoteOperation(ctx, remoteOperationListServicePrograms, remoteOperationResult(err), time.Since(start).Milliseconds())
	}()

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
	start := time.Now()
	defer func() {
		observability.RecordRemoteOperation(ctx, remoteOperationStreamProgramEvents, remoteOperationResult(err), time.Since(start).Milliseconds())
	}()

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

func (c *RemoteClient) stream(ctx context.Context, operation string, decode bool, dst io.Writer, elems ...string) (err error) {
	start := time.Now()
	defer func() {
		observability.RecordRemoteOperation(ctx, operation, remoteOperationResult(err), time.Since(start).Milliseconds())
	}()

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

func remoteOperationResult(err error) string {
	switch {
	case err == nil:
		return "success"
	case errors.Is(err, ErrChannelNotFound):
		return "not_found"
	case errors.Is(err, ErrTunerUnavailable):
		return "unavailable"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "canceled"
	default:
		return "failure"
	}
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
