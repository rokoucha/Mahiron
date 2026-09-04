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

const (
	// defaultRemoteAvailabilityTimeout bounds the tuner availability check that
	// runs before every remote service scan and stream. Mirakurun's /api/tuners
	// gets slower as the tuner count and the load of the remote grow, so this is
	// more generous than the status probe below. Override it per remote with
	// availabilityTimeout in remotes.yml.
	defaultRemoteAvailabilityTimeout = 5 * time.Second
	// defaultRemoteStatusTimeout keeps a temporarily unreachable remote from
	// delaying the local status page.
	defaultRemoteStatusTimeout = 3 * time.Second
)

const (
	remoteOperationCheckAvailable      = "remote.check_available"
	remoteOperationChannelStream       = "remote.channel_stream"
	remoteOperationServiceStream       = "remote.service_stream"
	remoteOperationProgramStream       = "remote.program_stream"
	remoteOperationScanServices        = "remote.scan_services"
	remoteOperationListServicePrograms = "remote.list_service_programs"
	remoteOperationGetLogoImage        = "remote.get_logo_image"
)

type Client struct {
	baseURL             string
	basicAuth           *config.BasicAuthConfig
	httpClient          *http.Client
	availabilityTimeout time.Duration
	statusTimeout       time.Duration
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
	availabilityTimeout := defaultRemoteAvailabilityTimeout
	statusTimeout := defaultRemoteStatusTimeout
	if config.AvailabilityTimeout > 0 {
		availabilityTimeout = time.Duration(config.AvailabilityTimeout) * time.Millisecond
		// A remote slow enough to need a longer availability check answers the
		// status page from the same endpoint, so honor the override there too.
		statusTimeout = availabilityTimeout
	}
	client := &Client{
		baseURL:             strings.TrimRight(config.URL, "/"),
		basicAuth:           config.BasicAuth,
		httpClient:          http.DefaultClient,
		availabilityTimeout: availabilityTimeout,
		statusTimeout:       statusTimeout,
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

	checkCtx, cancel := context.WithTimeout(ctx, c.availabilityTimeout)
	defer cancel()

	req, err := c.newRequest(checkCtx, http.MethodHead, "channels", channelType, channel, "stream")
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	case http.StatusNotFound:
		return ErrChannelNotFound
	case http.StatusServiceUnavailable:
		return tuner.ErrTunerUnavailable
	case http.StatusMethodNotAllowed, http.StatusNotImplemented:
		return c.checkAvailableFromTuners(checkCtx, channelType, channel)
	default:
		return remoteStatusError(resp.StatusCode, resp.Status)
	}
}

// checkAvailableFromTuners preserves compatibility with Mirakurun-compatible
// servers that do not implement HEAD on channel streams.
func (c *Client) checkAvailableFromTuners(ctx context.Context, channelType, channel string) error {
	req, err := c.newRequest(ctx, http.MethodGet, "tuners")
	if err != nil {
		return err
	}
	requestLocalTunersOnly(req)
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
	for _, item := range tuners {
		if slices.Contains(item.Types, channelType) && item.IsAvailable && !item.IsFault &&
			(item.IsFree || item.matchesRoute(channelType, channel)) {
			return nil
		}
	}
	return tuner.ErrTunerUnavailable
}

// TunerStatuses returns the current tuner state reported by the remote server.
// A short timeout keeps a temporarily unreachable remote from delaying the
// local status page indefinitely, unless the remote raised it with
// availabilityTimeout.
func (c *Client) TunerStatuses(ctx context.Context) ([]tuner.Status, error) {
	checkCtx, cancel := context.WithTimeout(ctx, c.statusTimeout)
	defer cancel()

	req, err := c.newRequest(checkCtx, http.MethodGet, "tuners")
	if err != nil {
		return nil, err
	}
	requestLocalTunersOnly(req)
	var remoteTuners []remoteTuner
	if err := c.doJSON(req, &remoteTuners); err != nil {
		return nil, err
	}
	statuses := make([]tuner.Status, len(remoteTuners))
	for i, item := range remoteTuners {
		statuses[i] = item.Status()
	}
	return statuses, nil
}

func requestLocalTunersOnly(req *http.Request) {
	query := req.URL.Query()
	query.Set("includeRemote", "0")
	req.URL.RawQuery = query.Encode()
}

func (c *Client) ChannelStream(ctx context.Context, channelType, channel string, decode bool, dst io.Writer) error {
	return c.stream(ctx, remoteOperationChannelStream, decode, dst, "channels", channelType, channel, "stream")
}

func (c *Client) ServiceStream(ctx context.Context, channelType, channel string, serviceID uint16, decode bool, dst io.Writer) error {
	serviceItemID, err := c.channelServiceItemID(ctx, channelType, channel, serviceID)
	if err != nil {
		return err
	}
	return c.stream(ctx, remoteOperationServiceStream, decode, dst, "services", fmt.Sprint(serviceItemID), "stream")
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
	// Use Mirakurun's standard service filter instead of Mahiron's newer
	// /channels/{type}/{channel}/services endpoint. Some older Mahiron servers
	// redirect that endpoint with a relative Location header, which net/http
	// resolves below the channel path and consequently turns into a 404.
	req, err := c.newRequest(ctx, http.MethodGet, "services")
	if err != nil {
		return nil, err
	}
	query := req.URL.Query()
	query.Set("channel.type", channelType)
	query.Set("channel.channel", channel)
	req.URL.RawQuery = query.Encode()
	if err := c.doJSON(req, &services); err != nil {
		return nil, err
	}
	return services, nil
}

func (c *Client) channelServiceItemID(ctx context.Context, channelType, channel string, serviceID uint16) (int64, error) {
	services, err := c.ListChannelServices(ctx, channelType, channel)
	if err != nil {
		return 0, err
	}
	for _, svc := range services {
		if svc.ServiceID == serviceID {
			return int64(svc.NetworkID)*100000 + int64(svc.ServiceID), nil
		}
	}
	return 0, ErrChannelNotFound
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

// StreamEvents subscribes once to the unfiltered Mirakurun-compatible event
// stream and dispatches the resource types Mahiron uses.
func (c *Client) StreamEvents(ctx context.Context, connected func(), updater ProgramUpdater, updateTuner func(string, tuner.Status)) error {
	req, err := c.newRequest(ctx, http.MethodGet, "events", "stream")
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
	connected()
	return readRemoteEvents(ctx, resp.Body, updater, updateTuner)
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
