package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/internal/db/gen"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/observability"
)

type sqliteStore struct {
	write *sql.DB
	read  *sql.DB
	wq    *gen.Queries
	rq    *gen.Queries
}

func NewSQLiteStore(database *db.DB) Store {
	return &sqliteStore{
		write: database.Write,
		read:  database.Read,
		wq:    gen.New(database.Write),
		rq:    gen.New(database.Read),
	}
}

func (s *sqliteStore) List(ctx context.Context) ([]*Service, error) {
	svcs, err := s.rq.ListServices(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]*Service, len(svcs))
	for i := range svcs {
		result[i] = fromServiceRow(listServicesRow(svcs[i]))
	}
	return result, nil
}

func (s *sqliteStore) Count(ctx context.Context) (int, error) {
	n, err := s.rq.CountServices(ctx)
	if err != nil {
		return 0, err
	}
	return int(n), nil
}

func (s *sqliteStore) GetByID(ctx context.Context, id string) (*Service, error) {
	svc, err := s.rq.GetServiceByID(ctx, id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return fromServiceRow(getServiceByIDRow(svc)), nil
}

func (s *sqliteStore) GetByItemID(ctx context.Context, itemID int64) (*Service, error) {
	svc, err := s.rq.GetServiceByItemID(ctx, itemID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return fromServiceRow(getServiceByItemIDRow(svc)), nil
}

func (s *sqliteStore) GetByNetworkServiceID(ctx context.Context, networkID, serviceID uint16) (*Service, error) {
	svc, err := s.rq.GetServiceByNetworkServiceID(ctx, gen.GetServiceByNetworkServiceIDParams{
		NetworkID: int64(networkID),
		ServiceID: int64(serviceID),
	})
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return fromServiceRow(getServiceByNetworkServiceIDRow(svc)), nil
}

func (s *sqliteStore) GetByChannel(ctx context.Context, channelType, channelId string) ([]*Service, error) {
	svcs, err := s.rq.GetServicesByChannel(ctx, gen.GetServicesByChannelParams{
		ChannelType: channelType,
		ChannelID:   channelId,
	})
	if err != nil {
		return nil, err
	}
	result := make([]*Service, len(svcs))
	for i := range svcs {
		result[i] = fromServiceRow(getServicesByChannelRow(svcs[i]))
	}
	return result, nil
}

func (s *sqliteStore) GetByChannelAndID(ctx context.Context, channelType, channelId string, id string, itemID int64) (*Service, error) {
	svc, err := s.rq.GetServiceByChannelAndID(ctx, gen.GetServiceByChannelAndIDParams{
		ChannelType: channelType,
		ChannelID:   channelId,
		ID:          id,
		ItemID:      itemID,
	})
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return fromServiceRow(getServiceByChannelAndIDRow(svc)), nil
}

func (s *sqliteStore) GetByTriplet(ctx context.Context, networkID, transportStreamID, serviceID uint16) (*Service, error) {
	svc, err := s.rq.GetServiceByTriplet(ctx, gen.GetServiceByTripletParams{
		NetworkID:         int64(networkID),
		TransportStreamID: int64(transportStreamID),
		ServiceID:         int64(serviceID),
	})
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return fromServiceRow(getServiceByTripletRow(svc)), nil
}

func (s *sqliteStore) GetLogoByServiceItemID(ctx context.Context, itemID int64) ([]byte, error) {
	data, err := s.rq.GetLogoByServiceItemID(ctx, itemID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (s *sqliteStore) KnownLogoTargets(ctx context.Context) ([]LogoTarget, error) {
	rows, err := s.rq.KnownLogoTargets(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]LogoTarget, 0, len(rows))
	for _, row := range rows {
		if row.LogoID == nil || row.LogoVersion == nil || row.LogoDownloadDataID == nil {
			continue
		}
		result = append(result, LogoTarget{
			NetworkId:          uint16(row.NetworkID),
			ServiceId:          uint16(row.ServiceID),
			TransportStreamId:  uint16(row.TransportStreamID),
			ChannelType:        row.ChannelType,
			ChannelId:          row.ChannelID,
			LogoId:             *row.LogoID,
			LogoVersion:        *row.LogoVersion,
			LogoDownloadDataId: *row.LogoDownloadDataID,
		})
	}
	return result, nil
}

func (s *sqliteStore) MissingLogoTargets(ctx context.Context) ([]LogoTarget, error) {
	rows, err := s.rq.MissingLogoTargets(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]LogoTarget, 0, len(rows))
	for _, row := range rows {
		if row.LogoID == nil || row.LogoVersion == nil || row.LogoDownloadDataID == nil {
			continue
		}
		result = append(result, LogoTarget{
			NetworkId: uint16(row.NetworkID), ServiceId: uint16(row.ServiceID), TransportStreamId: uint16(row.TransportStreamID),
			ChannelType: row.ChannelType, ChannelId: row.ChannelID, LogoId: *row.LogoID,
			LogoVersion: *row.LogoVersion, LogoDownloadDataId: *row.LogoDownloadDataID,
		})
	}
	return result, nil
}

func (s *sqliteStore) ListCommonDataAnnouncements(ctx context.Context) ([]CommonDataAnnouncement, error) {
	rows, err := s.rq.ListCommonDataAnnouncements(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]CommonDataAnnouncement, 0, len(rows))
	for _, row := range rows {
		result = append(result, CommonDataAnnouncement{
			OriginalNetworkID:   uint16(row.OriginalNetworkID),
			TransportStreamID:   uint16(row.TransportStreamID),
			ServiceID:           uint16(row.ServiceID),
			DownloadID:          uint32(row.DownloadID),
			VersionID:           uint16(row.VersionID),
			ObservedChannelType: row.ObservedChannelType,
			ObservedChannelID:   row.ObservedChannelID,
			SeenAt:              row.SeenAt,
		})
	}
	return result, nil
}

func (s *sqliteStore) UpsertCommonDataAnnouncement(ctx context.Context, announcement CommonDataAnnouncement) error {
	return s.wq.UpsertCommonDataAnnouncement(ctx, gen.UpsertCommonDataAnnouncementParams{
		OriginalNetworkID:   int64(announcement.OriginalNetworkID),
		TransportStreamID:   int64(announcement.TransportStreamID),
		ServiceID:           int64(announcement.ServiceID),
		DownloadID:          int64(announcement.DownloadID),
		VersionID:           int64(announcement.VersionID),
		ObservedChannelType: announcement.ObservedChannelType,
		ObservedChannelID:   announcement.ObservedChannelID,
		SeenAt:              announcement.SeenAt,
	})
}

func (s *sqliteStore) SetEPGAttempt(ctx context.Context, networkID, serviceID uint16, attemptedAt int64, lastError string) error {
	return s.wq.SetEPGAttempt(ctx, gen.SetEPGAttemptParams{
		NetworkID:     int64(networkID),
		ServiceID:     int64(serviceID),
		LastAttemptAt: &attemptedAt,
		LastError:     nullableString(lastError),
	})
}

func (s *sqliteStore) SetEPGSuccess(ctx context.Context, networkID, serviceID uint16, succeededAt int64) error {
	return s.wq.SetEPGSuccess(ctx, gen.SetEPGSuccessParams{
		NetworkID:     int64(networkID),
		ServiceID:     int64(serviceID),
		LastAttemptAt: &succeededAt,
		LastSuccessAt: &succeededAt,
	})
}

func (s *sqliteStore) UpdateServiceLogoMetadata(ctx context.Context, networkID, transportStreamID, serviceID uint16, logoID, logoVersion, downloadDataID int64) (bool, error) {
	rows, err := s.wq.UpdateServiceLogoMetadata(ctx, gen.UpdateServiceLogoMetadataParams{
		LogoID:             &logoID,
		LogoVersion:        &logoVersion,
		LogoDownloadDataID: &downloadDataID,
		NetworkID:          int64(networkID),
		TransportStreamID:  int64(transportStreamID),
		ServiceID:          int64(serviceID),
	})
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

func (s *sqliteStore) DeleteLogo(ctx context.Context, networkID, transportStreamID, serviceID uint16, logoID int64, logoType int64, logoVersion int64, downloadDataID int64) error {
	return s.wq.DeleteServiceLogo(ctx, gen.DeleteServiceLogoParams{
		NetworkID:         int64(networkID),
		TransportStreamID: int64(transportStreamID),
		ServiceID:         int64(serviceID),
		LogoID:            logoID,
		LogoType:          logoType,
		LogoVersion:       logoVersion,
		DownloadDataID:    downloadDataID,
	})
}

func (s *sqliteStore) UpsertLogo(ctx context.Context, networkID, transportStreamID, serviceID uint16, logoID int64, logoType int64, logoVersion int64, downloadDataID int64, data []byte, updatedAt int64) error {
	return s.wq.UpsertServiceLogo(ctx, gen.UpsertServiceLogoParams{
		NetworkID:         int64(networkID),
		TransportStreamID: int64(transportStreamID),
		ServiceID:         int64(serviceID),
		LogoID:            logoID,
		LogoType:          logoType,
		LogoVersion:       logoVersion,
		DownloadDataID:    downloadDataID,
		Data:              data,
		UpdatedAt:         updatedAt,
	})
}

func (s *sqliteStore) EPGSummary(ctx context.Context, staleAfter int64, now int64) (stale, failed int, lastSuccess *int64, err error) {
	start := time.Now()
	ctx, span := observability.StartSpan(ctx, observability.SpanDBServiceEPGSummary,
		observability.AttrEPGStaleAfter.Int64(staleAfter),
	)
	defer func() {
		observability.RecordDBOperation(ctx, observability.SpanDBServiceEPGSummary, time.Since(start).Milliseconds(), err)
		observability.EndSpan(span, err)
	}()

	row, err := s.rq.GetEPGSummary(ctx, gen.GetEPGSummaryParams{
		Now:        &now,
		StaleAfter: &staleAfter,
	})
	if err != nil {
		return 0, 0, nil, err
	}
	lastSuccess, err = nullableInt64(row.LastSuccessAt)
	if err != nil {
		return 0, 0, nil, err
	}
	return int(row.Stale), int(row.Failed), lastSuccess, nil
}

func nullableInt64(value any) (*int64, error) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case int64:
		return &v, nil
	case []byte:
		n, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return nil, err
		}
		return &n, nil
	case string:
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, err
		}
		return &n, nil
	default:
		return nil, fmt.Errorf("unexpected nullable int64 type %T", value)
	}
}

func nullableString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func (s *sqliteStore) ReplaceChannelServices(ctx context.Context, channelType, channelId string, services []*Service) (err error) {
	start := time.Now()
	ctx, span := observability.StartSpan(ctx, observability.SpanDBServiceReplaceChannelServices,
		observability.AttrChannelType.String(channelType),
		observability.AttrChannelID.String(channelId),
		observability.AttrServiceCount.Int(len(services)),
	)
	defer func() {
		observability.RecordDBOperation(ctx, observability.SpanDBServiceReplaceChannelServices, time.Since(start).Milliseconds(), err)
		observability.EndSpan(span, err)
	}()

	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, tx.Rollback())
		}
	}()

	q := s.wq.WithTx(tx)
	existingRows, err := q.GetServicesByChannel(ctx, gen.GetServicesByChannelParams{
		ChannelType: channelType,
		ChannelID:   channelId,
	})
	if err != nil {
		return fmt.Errorf("load existing services: %w", err)
	}
	existingLogos := make(map[model.ServiceKey]model.LogoRef, len(existingRows))
	for _, row := range existingRows {
		svc := fromServiceRow(getServicesByChannelRow(row))
		if svc.Logo == nil || svc.Logo.Version == nil || svc.Logo.DownloadDataID == nil {
			continue
		}
		existingLogos[svc.Key] = *svc.Logo
	}

	if err := q.DeleteServicesByChannel(ctx, gen.DeleteServicesByChannelParams{
		ChannelType: channelType,
		ChannelID:   channelId,
	}); err != nil {
		return fmt.Errorf("delete existing: %w", err)
	}

	for _, svc := range services {
		preserveServiceLogoMetadata(svc, existingLogos)
		if err := q.UpsertService(ctx, upsertServiceParams(svc, channelType, channelId)); err != nil {
			return fmt.Errorf("upsert service %s: %w", svc.Id, err)
		}
	}

	return tx.Commit()
}

func upsertServiceParams(svc *Service, channelType, channelId string) gen.UpsertServiceParams {
	params := gen.UpsertServiceParams{
		ID:                  svc.Id,
		ServiceID:           int64(svc.Key.ServiceID),
		NetworkID:           int64(svc.Key.NetworkID),
		TransportStreamID:   int64(svc.Key.StreamID),
		Name:                svc.Name,
		ProviderName:        svc.ProviderName,
		Type:                int64(svc.Type),
		RunningStatus:       int64(svc.RunningStatus),
		FreeCa:              boolToInt64(svc.FreeCA),
		EitScheduleFlag:     boolToInt64(svc.EITSchedule),
		EitPresentFollowing: boolToInt64(svc.EITPresentFollow),
		RemoteControlKeyID:  uint8Ptr64(svc.RemoteControlKey),
		ChannelType:         channelType,
		ChannelID:           channelId,
	}
	if logo := svc.Logo; logo != nil {
		logoID := int64(logo.LogoID)
		params.LogoID = &logoID
		params.LogoVersion = uint16Ptr64(logo.Version)
		params.LogoDownloadDataID = uint16Ptr64(logo.DownloadDataID)
		if logo.HasSimpleLogo {
			params.SimpleLogo = &logo.SimpleLogo
		}
	}
	return params
}

// preserveServiceLogoMetadata keeps the logo a previous scan resolved when
// this scan could not resolve it, e.g. when the SDT carried only an
// indirect reference.
func preserveServiceLogoMetadata(svc *Service, existing map[model.ServiceKey]model.LogoRef) {
	// A simple logo is complete in the SDT and has no CDT logo to keep.
	if svc.Logo != nil && svc.Logo.HasSimpleLogo {
		return
	}
	previous, ok := existing[svc.Key]
	if !ok {
		return
	}
	if svc.Logo == nil {
		svc.Logo = &previous
		return
	}
	if svc.Logo.Version == nil {
		svc.Logo.Version = previous.Version
	}
	if svc.Logo.DownloadDataID == nil {
		svc.Logo.DownloadDataID = previous.DownloadDataID
	}
}

func (s *sqliteStore) PruneChannels(ctx context.Context, active []ChannelKey) (err error) {
	allowed := make(map[ChannelKey]struct{}, len(active))
	for _, key := range active {
		allowed[key] = struct{}{}
	}
	services, err := s.rq.ListServices(ctx)
	if err != nil {
		return fmt.Errorf("list services: %w", err)
	}
	stale := make(map[ChannelKey]struct{})
	for _, svc := range services {
		key := ChannelKey{Type: svc.Service.ChannelType, ID: svc.Service.ChannelID}
		if _, ok := allowed[key]; !ok {
			stale[key] = struct{}{}
		}
	}
	if len(stale) == 0 {
		return nil
	}
	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin prune tx: %w", err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, tx.Rollback())
		}
	}()
	q := s.wq.WithTx(tx)
	for key := range stale {
		if err := q.DeleteServicesByChannel(ctx, gen.DeleteServicesByChannelParams{ChannelType: key.Type, ChannelID: key.ID}); err != nil {
			return fmt.Errorf("delete stale channel %s/%s: %w", key.Type, key.ID, err)
		}
	}
	return tx.Commit()
}

type serviceRow struct {
	service       gen.Service
	hasLogoData   bool
	lastAttemptAt *int64
	lastSuccessAt *int64
	lastError     *string
}

func fromServiceRow(row serviceRow) *Service {
	s := row.service
	result := &Service{
		Id: s.ID,
		Service: model.Service{
			Key: model.ServiceKey{
				NetworkID: uint16(s.NetworkID),
				StreamID:  uint16(s.TransportStreamID),
				ServiceID: uint16(s.ServiceID),
			},
			Name:             s.Name,
			ProviderName:     s.ProviderName,
			Type:             uint8(s.Type),
			RunningStatus:    uint8(s.RunningStatus),
			FreeCA:           s.FreeCa != 0,
			EITSchedule:      s.EitScheduleFlag != 0,
			EITPresentFollow: s.EitPresentFollowing != 0,
		},
		HasLogoData: row.hasLogoData,
		ChannelType: s.ChannelType,
		ChannelId:   s.ChannelID,
		EPG: EPGStatus{
			LastAttemptAt: row.lastAttemptAt,
			LastSuccessAt: row.lastSuccessAt,
		},
	}
	if s.RemoteControlKeyID != nil {
		key := uint8(*s.RemoteControlKeyID)
		result.RemoteControlKey = &key
	}
	if s.LogoID != nil {
		logo := &model.LogoRef{LogoID: uint16(*s.LogoID)}
		if s.LogoVersion != nil {
			version := uint16(*s.LogoVersion)
			logo.Version = &version
		}
		if s.LogoDownloadDataID != nil {
			downloadDataID := uint16(*s.LogoDownloadDataID)
			logo.DownloadDataID = &downloadDataID
		}
		if s.SimpleLogo != nil {
			logo.SimpleLogo, logo.HasSimpleLogo = *s.SimpleLogo, true
		}
		result.Logo = logo
	}
	if row.lastError != nil {
		result.EPG.LastError = *row.lastError
	}
	return result
}

func listServicesRow(s gen.ListServicesRow) serviceRow {
	return serviceRow{s.Service, s.HasLogoData, s.LastAttemptAt, s.LastSuccessAt, s.LastError}
}

func getServiceByIDRow(s gen.GetServiceByIDRow) serviceRow {
	return serviceRow{s.Service, s.HasLogoData, s.LastAttemptAt, s.LastSuccessAt, s.LastError}
}

func getServiceByItemIDRow(s gen.GetServiceByItemIDRow) serviceRow {
	return serviceRow{s.Service, s.HasLogoData, s.LastAttemptAt, s.LastSuccessAt, s.LastError}
}

func getServiceByNetworkServiceIDRow(s gen.GetServiceByNetworkServiceIDRow) serviceRow {
	return serviceRow{s.Service, s.HasLogoData, s.LastAttemptAt, s.LastSuccessAt, s.LastError}
}

func getServicesByChannelRow(s gen.GetServicesByChannelRow) serviceRow {
	return serviceRow{s.Service, s.HasLogoData, s.LastAttemptAt, s.LastSuccessAt, s.LastError}
}

func getServiceByChannelAndIDRow(s gen.GetServiceByChannelAndIDRow) serviceRow {
	return serviceRow{s.Service, s.HasLogoData, s.LastAttemptAt, s.LastSuccessAt, s.LastError}
}

func getServiceByTripletRow(s gen.GetServiceByTripletRow) serviceRow {
	return serviceRow{s.Service, s.HasLogoData, s.LastAttemptAt, s.LastSuccessAt, s.LastError}
}

func boolToInt64(value bool) int64 {
	if value {
		return 1
	}
	return 0
}

func uint8Ptr64(value *uint8) *int64 {
	if value == nil {
		return nil
	}
	v := int64(*value)
	return &v
}

func uint16Ptr64(value *uint16) *int64 {
	if value == nil {
		return nil
	}
	v := int64(*value)
	return &v
}
