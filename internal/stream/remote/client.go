package remote

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
	"github.com/21S1298001/mahiron/internal/tuner"
	"github.com/21S1298001/mahiron/ts"
)

const xMirakurunTunerUserID = "X-Mirakurun-Tuner-User-ID"

const remoteAvailabilityTimeout = 3 * time.Second

const (
	remoteOperationCheckAvailable      = "remote.check_available"
	remoteOperationChannelStream       = "remote.channel_stream"
	remoteOperationServiceStream       = "remote.service_stream"
	remoteOperationProgramStream       = "remote.program_stream"
	remoteOperationScanServices        = "remote.scan_services"
	remoteOperationListServicePrograms = "remote.list_service_programs"
	remoteOperationStreamProgramEvents = "remote.stream_program_events"
	remoteOperationGetLogoImage        = "remote.get_logo_image"
)

type Client struct {
	baseURL    string
	basicAuth  *config.BasicAuthConfig
	httpClient *http.Client
}

type ProgramUpdater interface {
	UpsertPrograms(context.Context, []*program.Program) error
}

// ClientOption customizes a Client created by NewClient.
type ClientOption func(*Client)

// WithHTTPClient replaces the HTTP client used for upstream requests.
// It exists mainly so tests can inject a stub transport.
func WithHTTPClient(httpClient *http.Client) ClientOption {
	return func(c *Client) {
		c.httpClient = httpClient
	}
}

func NewClient(config config.RemoteConfig, opts ...ClientOption) *Client {
	client := &Client{
		baseURL:    strings.TrimRight(config.URL, "/"),
		basicAuth:  config.BasicAuth,
		httpClient: http.DefaultClient,
	}
	for _, opt := range opts {
		opt(client)
	}
	return client
}

func (c *Client) CheckAvailableForRoute(ctx context.Context, channelType, channel string) (err error) {
	start := time.Now()
	defer func() {
		observability.RecordRemoteOperation(ctx, remoteOperationCheckAvailable, remoteOperationResult(err), time.Since(start).Milliseconds())
	}()

	checkCtx, cancel := context.WithTimeout(ctx, remoteAvailabilityTimeout)
	defer cancel()

	req, err := c.newRequest(checkCtx, http.MethodGet, "tuners")
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := remoteStatusError(resp.StatusCode, resp.Status); err != nil {
		return err
	}

	var tuners []remoteTuner
	if err := json.NewDecoder(resp.Body).Decode(&tuners); err != nil {
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
	return tuner.ErrTunerUnavailable
}

func (c *Client) ChannelStream(ctx context.Context, channelType, channel string, decode bool, dst io.Writer) error {
	return c.stream(ctx, remoteOperationChannelStream, decode, dst, "channels", channelType, channel, "stream")
}

func (c *Client) ServiceStream(ctx context.Context, channelType, channel string, serviceID uint16, decode bool, dst io.Writer) error {
	return c.stream(ctx, remoteOperationServiceStream, decode, dst, "channels", channelType, channel, "services", fmt.Sprint(serviceID), "stream")
}

func (c *Client) ProgramStream(ctx context.Context, programID int64, decode bool, dst io.Writer) error {
	return c.stream(ctx, remoteOperationProgramStream, decode, dst, "programs", fmt.Sprint(programID), "stream")
}

func (c *Client) GetLogoImage(ctx context.Context, serviceItemID int64) (data []byte, err error) {
	start := time.Now()
	defer func() {
		observability.RecordRemoteOperation(ctx, remoteOperationGetLogoImage, remoteOperationResult(err), time.Since(start).Milliseconds())
	}()

	req, err := c.newRequest(ctx, http.MethodGet, "services", fmt.Sprint(serviceItemID), "logo")
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if err := remoteStatusError(resp.StatusCode, resp.Status); err != nil {
		return nil, err
	}
	return io.ReadAll(resp.Body)
}

func (c *Client) ScanServices(ctx context.Context, channelType, channel string) (scanned []ts.ServiceInfo, err error) {
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

	services, err := c.ListChannelServices(ctx, channelType, channel)
	if err != nil {
		return nil, err
	}
	scanned = make([]ts.ServiceInfo, len(services))
	for i, svc := range services {
		logoID := int64(-1)
		var logoVersion *uint16
		var logoDownloadDataID *uint16
		if remoteServiceHasLogo(svc) {
			logoID = *svc.LogoID
			logoVersion = remoteLogoVersion()
			logoDownloadDataID = remoteLogoDownloadDataID(svc)
		}
		scanned[i] = ts.ServiceInfo{
			Nid:                 svc.NetworkID,
			Tsid:                svc.TransportStreamID,
			Sid:                 svc.ServiceID,
			Name:                svc.Name,
			Type:                uint8(svc.Type),
			EITScheduleFlag:     remoteBoolDefault(svc.EITScheduleFlag, true),
			EITPresentFollowing: remoteBoolDefault(svc.EITPresentFollowing, true),
			LogoId:              logoID,
			LogoVersion:         logoVersion,
			LogoDownloadDataId:  logoDownloadDataID,
			RemoteControlKeyId:  uint8Ptr(uint8(svc.RemoteControlKeyID)),
		}
	}
	return scanned, nil
}

func remoteBoolDefault(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}

func (c *Client) ListChannelServices(ctx context.Context, channelType, channel string) ([]remoteService, error) {
	var services []remoteService
	if err := c.getJSON(ctx, &services, "channels", channelType, channel, "services"); err != nil {
		return nil, err
	}
	return services, nil
}

func remoteServiceHasLogo(svc remoteService) bool {
	return svc.LogoID != nil && *svc.LogoID >= 0 && svc.HasLogoData
}

func remoteLogoVersion() *uint16 {
	version := uint16(0)
	return &version
}

func remoteLogoDownloadDataID(svc remoteService) *uint16 {
	downloadDataID := svc.ServiceID
	return &downloadDataID
}

func (c *Client) ListServicePrograms(ctx context.Context, networkID, serviceID uint16) (programs []*program.Program, err error) {
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

func (c *Client) StreamProgramEvents(ctx context.Context, updater ProgramUpdater) (err error) {
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

func (c *Client) stream(ctx context.Context, operation string, decode bool, dst io.Writer, elems ...string) (err error) {
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
	if user, ok := tuner.UserFromContext(ctx); ok {
		req.Header.Set("X-Mirakurun-Priority", fmt.Sprint(user.Priority))
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := remoteStatusError(resp.StatusCode, resp.Status); err != nil {
		return err
	}
	if userID := resp.Header.Get(xMirakurunTunerUserID); userID != "" {
		slog.Debug("remote stream acquired tuner user", "remote", c.baseURL, "userId", userID)
	}
	_, err = io.Copy(dst, resp.Body)
	return err
}

func remoteStatusError(statusCode int, status string) error {
	if statusCode >= 200 && statusCode < 300 {
		return nil
	}
	switch statusCode {
	case http.StatusNotFound:
		return ErrChannelNotFound
	case http.StatusConflict, http.StatusLocked, http.StatusServiceUnavailable:
		return tuner.ErrTunerUnavailable
	default:
		return fmt.Errorf("remote API status: %s", status)
	}
}

func remoteOperationResult(err error) string {
	switch {
	case err == nil:
		return "success"
	case errors.Is(err, ErrChannelNotFound):
		return "not_found"
	case errors.Is(err, tuner.ErrTunerUnavailable):
		return "unavailable"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "canceled"
	default:
		return "failure"
	}
}

func (c *Client) getJSON(ctx context.Context, dst any, elems ...string) error {
	req, err := c.newRequest(ctx, http.MethodGet, elems...)
	if err != nil {
		return err
	}
	return c.doJSON(req, dst)
}

func (c *Client) doJSON(req *http.Request, dst any) error {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := remoteStatusError(resp.StatusCode, resp.Status); err != nil {
		return err
	}
	return json.NewDecoder(resp.Body).Decode(dst)
}

func (c *Client) newRequest(ctx context.Context, method string, elems ...string) (*http.Request, error) {
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
