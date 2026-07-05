package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/21S1298001/mahiron/internal/db/gen"
	"github.com/21S1298001/mahiron/internal/observability"
)

type sqliteStore struct {
	db *sql.DB
	q  *gen.Queries
}

func NewSQLiteStore(db *sql.DB) Store {
	return &sqliteStore{
		db: db,
		q:  gen.New(db),
	}
}

func (s *sqliteStore) List(ctx context.Context) ([]*Service, error) {
	svcs, err := s.q.ListServices(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]*Service, len(svcs))
	for i := range svcs {
		result[i] = fromServiceRow(listServiceRow(svcs[i]))
	}
	return result, nil
}

func (s *sqliteStore) Count(ctx context.Context) (int, error) {
	n, err := s.q.CountServices(ctx)
	if err != nil {
		return 0, err
	}
	return int(n), nil
}

func (s *sqliteStore) GetByID(ctx context.Context, id string) (*Service, error) {
	svc, err := s.q.GetServiceByID(ctx, id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return fromServiceRow(getServiceByIDRow(svc)), nil
}

func (s *sqliteStore) GetByItemID(ctx context.Context, itemID int64) (*Service, error) {
	svc, err := s.q.GetServiceByItemID(ctx, itemID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return fromServiceRow(getServiceByItemIDRow(svc)), nil
}

func (s *sqliteStore) GetByNetworkServiceID(ctx context.Context, networkID, serviceID uint16) (*Service, error) {
	svc, err := s.q.GetServiceByNetworkServiceID(ctx, gen.GetServiceByNetworkServiceIDParams{
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
	svcs, err := s.q.GetServicesByChannel(ctx, gen.GetServicesByChannelParams{
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
	svc, err := s.q.GetServiceByChannelAndID(ctx, gen.GetServiceByChannelAndIDParams{
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
	svc, err := s.q.GetServiceByTriplet(ctx, gen.GetServiceByTripletParams{
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
	data, err := s.q.GetLogoByServiceItemID(ctx, itemID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (s *sqliteStore) KnownLogoTargets(ctx context.Context) ([]LogoTarget, error) {
	rows, err := s.q.KnownLogoTargets(ctx)
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
	rows, err := s.q.MissingLogoTargets(ctx)
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
	rows, err := s.q.ListCommonDataAnnouncements(ctx)
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
	return s.q.UpsertCommonDataAnnouncement(ctx, gen.UpsertCommonDataAnnouncementParams{
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
	return s.q.SetEPGAttempt(ctx, gen.SetEPGAttemptParams{
		NetworkID:     int64(networkID),
		ServiceID:     int64(serviceID),
		LastAttemptAt: &attemptedAt,
		LastError:     nullableString(lastError),
	})
}

func (s *sqliteStore) SetEPGSuccess(ctx context.Context, networkID, serviceID uint16, succeededAt int64) error {
	return s.q.SetEPGSuccess(ctx, gen.SetEPGSuccessParams{
		NetworkID:     int64(networkID),
		ServiceID:     int64(serviceID),
		LastAttemptAt: &succeededAt,
		LastSuccessAt: &succeededAt,
	})
}

func (s *sqliteStore) UpdateServiceLogoMetadata(ctx context.Context, networkID, transportStreamID, serviceID uint16, logoID, logoVersion, downloadDataID int64) (bool, error) {
	rows, err := s.q.UpdateServiceLogoMetadata(ctx, gen.UpdateServiceLogoMetadataParams{
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
	return s.q.DeleteServiceLogo(ctx, gen.DeleteServiceLogoParams{
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
	return s.q.UpsertServiceLogo(ctx, gen.UpsertServiceLogoParams{
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

	row, err := s.q.GetEPGSummary(ctx, gen.GetEPGSummaryParams{
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

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, tx.Rollback())
		}
	}()

	q := s.q.WithTx(tx)
	existingRows, err := q.GetServicesByChannel(ctx, gen.GetServicesByChannelParams{
		ChannelType: channelType,
		ChannelID:   channelId,
	})
	if err != nil {
		return fmt.Errorf("load existing services: %w", err)
	}
	existingLogos := make(map[serviceTriplet]logoMetadata, len(existingRows))
	for _, row := range existingRows {
		if row.LogoID == nil || row.LogoVersion == nil || row.LogoDownloadDataID == nil {
			continue
		}
		existingLogos[serviceTriplet{
			networkID:         uint16(row.NetworkID),
			transportStreamID: uint16(row.TransportStreamID),
			serviceID:         uint16(row.ServiceID),
		}] = logoMetadata{
			logoID:         *row.LogoID,
			logoVersion:    *row.LogoVersion,
			downloadDataID: *row.LogoDownloadDataID,
		}
	}

	if err := q.DeleteServicesByChannel(ctx, gen.DeleteServicesByChannelParams{
		ChannelType: channelType,
		ChannelID:   channelId,
	}); err != nil {
		return fmt.Errorf("delete existing: %w", err)
	}

	for _, svc := range services {
		preserveServiceLogoMetadata(svc, existingLogos)
		if err := q.UpsertService(ctx, gen.UpsertServiceParams{
			ID:                  svc.Id,
			ServiceID:           int64(svc.ServiceId),
			NetworkID:           int64(svc.NetworkId),
			TransportStreamID:   int64(svc.TransportStreamId),
			Name:                svc.Name,
			Type:                int64(svc.Type),
			EitScheduleFlag:     boolToInt64(svc.EITScheduleFlag),
			EitPresentFollowing: boolToInt64(svc.EITPresentFollowing),
			LogoID:              svc.LogoId,
			LogoVersion:         svc.LogoVersion,
			LogoDownloadDataID:  svc.LogoDownloadDataId,
			RemoteControlKeyID:  int64(svc.RemoteControlKeyId),
			ChannelType:         channelType,
			ChannelID:           channelId,
		}); err != nil {
			return fmt.Errorf("upsert service %s: %w", svc.Id, err)
		}
	}

	return tx.Commit()
}

type serviceTriplet struct {
	networkID, transportStreamID, serviceID uint16
}

type logoMetadata struct {
	logoID, logoVersion, downloadDataID int64
}

func preserveServiceLogoMetadata(svc *Service, existing map[serviceTriplet]logoMetadata) {
	if svc.LogoId != nil && svc.LogoVersion != nil && svc.LogoDownloadDataId != nil {
		return
	}
	metadata, ok := existing[serviceTriplet{
		networkID:         svc.NetworkId,
		transportStreamID: svc.TransportStreamId,
		serviceID:         svc.ServiceId,
	}]
	if !ok {
		return
	}
	if svc.LogoId == nil {
		v := metadata.logoID
		svc.LogoId = &v
	}
	if svc.LogoVersion == nil {
		v := metadata.logoVersion
		svc.LogoVersion = &v
	}
	if svc.LogoDownloadDataId == nil {
		v := metadata.downloadDataID
		svc.LogoDownloadDataId = &v
	}
}

func (s *sqliteStore) PruneChannels(ctx context.Context, active []ChannelKey) (err error) {
	allowed := make(map[ChannelKey]struct{}, len(active))
	for _, key := range active {
		allowed[key] = struct{}{}
	}
	services, err := s.q.ListServices(ctx)
	if err != nil {
		return fmt.Errorf("list services: %w", err)
	}
	stale := make(map[ChannelKey]struct{})
	for _, svc := range services {
		key := ChannelKey{Type: svc.ChannelType, ID: svc.ChannelID}
		if _, ok := allowed[key]; !ok {
			stale[key] = struct{}{}
		}
	}
	if len(stale) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin prune tx: %w", err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, tx.Rollback())
		}
	}()
	q := s.q.WithTx(tx)
	for key := range stale {
		if err := q.DeleteServicesByChannel(ctx, gen.DeleteServicesByChannelParams{ChannelType: key.Type, ChannelID: key.ID}); err != nil {
			return fmt.Errorf("delete stale channel %s/%s: %w", key.Type, key.ID, err)
		}
	}
	return tx.Commit()
}

type serviceRow struct {
	id                  string
	serviceID           int64
	networkID           int64
	transportStreamID   int64
	name                string
	typ                 int64
	eitScheduleFlag     int64
	eitPresentFollowing int64
	logoID              *int64
	logoVersion         *int64
	logoDownloadDataID  *int64
	hasLogoData         bool
	remoteControlKeyID  int64
	channelType         string
	channelID           string
	lastAttemptAt       *int64
	lastSuccessAt       *int64
	lastError           *string
}

func fromServiceRow(s serviceRow) *Service {
	result := &Service{
		Id:                  s.id,
		ServiceId:           uint16(s.serviceID),
		NetworkId:           uint16(s.networkID),
		TransportStreamId:   uint16(s.transportStreamID),
		Name:                s.name,
		Type:                uint8(s.typ),
		EITScheduleFlag:     s.eitScheduleFlag != 0,
		EITPresentFollowing: s.eitPresentFollowing != 0,
		LogoId:              s.logoID,
		LogoVersion:         s.logoVersion,
		LogoDownloadDataId:  s.logoDownloadDataID,
		HasLogoData:         s.hasLogoData,
		RemoteControlKeyId:  uint8(s.remoteControlKeyID),
		ChannelType:         s.channelType,
		ChannelId:           s.channelID,
		EPG: EPGStatus{
			LastAttemptAt: s.lastAttemptAt,
			LastSuccessAt: s.lastSuccessAt,
		},
	}
	if s.lastError != nil {
		result.EPG.LastError = *s.lastError
	}
	return result
}

func listServiceRow(s gen.ListServicesRow) serviceRow {
	return serviceRow{s.ID, s.ServiceID, s.NetworkID, s.TransportStreamID, s.Name, s.Type, s.EitScheduleFlag, s.EitPresentFollowing, s.LogoID, s.LogoVersion, s.LogoDownloadDataID, s.HasLogoData, s.RemoteControlKeyID, s.ChannelType, s.ChannelID, s.LastAttemptAt, s.LastSuccessAt, s.LastError}
}

func getServiceByIDRow(s gen.GetServiceByIDRow) serviceRow {
	return serviceRow{s.ID, s.ServiceID, s.NetworkID, s.TransportStreamID, s.Name, s.Type, s.EitScheduleFlag, s.EitPresentFollowing, s.LogoID, s.LogoVersion, s.LogoDownloadDataID, s.HasLogoData, s.RemoteControlKeyID, s.ChannelType, s.ChannelID, s.LastAttemptAt, s.LastSuccessAt, s.LastError}
}

func getServiceByItemIDRow(s gen.GetServiceByItemIDRow) serviceRow {
	return serviceRow{s.ID, s.ServiceID, s.NetworkID, s.TransportStreamID, s.Name, s.Type, s.EitScheduleFlag, s.EitPresentFollowing, s.LogoID, s.LogoVersion, s.LogoDownloadDataID, s.HasLogoData, s.RemoteControlKeyID, s.ChannelType, s.ChannelID, s.LastAttemptAt, s.LastSuccessAt, s.LastError}
}

func getServiceByNetworkServiceIDRow(s gen.GetServiceByNetworkServiceIDRow) serviceRow {
	return serviceRow{s.ID, s.ServiceID, s.NetworkID, s.TransportStreamID, s.Name, s.Type, s.EitScheduleFlag, s.EitPresentFollowing, s.LogoID, s.LogoVersion, s.LogoDownloadDataID, s.HasLogoData, s.RemoteControlKeyID, s.ChannelType, s.ChannelID, s.LastAttemptAt, s.LastSuccessAt, s.LastError}
}

func getServicesByChannelRow(s gen.GetServicesByChannelRow) serviceRow {
	return serviceRow{s.ID, s.ServiceID, s.NetworkID, s.TransportStreamID, s.Name, s.Type, s.EitScheduleFlag, s.EitPresentFollowing, s.LogoID, s.LogoVersion, s.LogoDownloadDataID, s.HasLogoData, s.RemoteControlKeyID, s.ChannelType, s.ChannelID, s.LastAttemptAt, s.LastSuccessAt, s.LastError}
}

func getServiceByChannelAndIDRow(s gen.GetServiceByChannelAndIDRow) serviceRow {
	return serviceRow{s.ID, s.ServiceID, s.NetworkID, s.TransportStreamID, s.Name, s.Type, s.EitScheduleFlag, s.EitPresentFollowing, s.LogoID, s.LogoVersion, s.LogoDownloadDataID, s.HasLogoData, s.RemoteControlKeyID, s.ChannelType, s.ChannelID, s.LastAttemptAt, s.LastSuccessAt, s.LastError}
}

func getServiceByTripletRow(s gen.GetServiceByTripletRow) serviceRow {
	return serviceRow{s.ID, s.ServiceID, s.NetworkID, s.TransportStreamID, s.Name, s.Type, s.EitScheduleFlag, s.EitPresentFollowing, s.LogoID, s.LogoVersion, s.LogoDownloadDataID, s.HasLogoData, s.RemoteControlKeyID, s.ChannelType, s.ChannelID, s.LastAttemptAt, s.LastSuccessAt, s.LastError}
}

func boolToInt64(value bool) int64 {
	if value {
		return 1
	}
	return 0
}
