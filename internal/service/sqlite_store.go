package service

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/21S1298001/Mahiron5/internal/db/gen"
	"github.com/21S1298001/Mahiron5/internal/observability"
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

func (s *sqliteStore) DeleteLogo(ctx context.Context, networkID, serviceID uint16, logoID int64, logoType int64, logoVersion int64, downloadDataID int64) error {
	return s.q.DeleteServiceLogo(ctx, gen.DeleteServiceLogoParams{
		NetworkID:      int64(networkID),
		ServiceID:      int64(serviceID),
		LogoID:         logoID,
		LogoType:       logoType,
		LogoVersion:    logoVersion,
		DownloadDataID: downloadDataID,
	})
}

func (s *sqliteStore) UpsertLogo(ctx context.Context, networkID, serviceID uint16, logoID int64, logoType int64, logoVersion int64, downloadDataID int64, data []byte, updatedAt int64) error {
	return s.q.UpsertServiceLogo(ctx, gen.UpsertServiceLogoParams{
		NetworkID:      int64(networkID),
		ServiceID:      int64(serviceID),
		LogoID:         logoID,
		LogoType:       logoType,
		LogoVersion:    logoVersion,
		DownloadDataID: downloadDataID,
		Data:           data,
		UpdatedAt:      updatedAt,
	})
}

func (s *sqliteStore) EPGSummary(ctx context.Context, staleAfter int64, now int64) (stale, failed int, lastSuccess *int64, err error) {
	ctx, span := observability.StartSpan(ctx, observability.SpanDBServiceEPGSummary,
		observability.AttrEPGStaleAfter.Int64(staleAfter),
	)
	defer func() { observability.EndSpan(span, err) }()

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
	ctx, span := observability.StartSpan(ctx, observability.SpanDBServiceReplaceChannelServices,
		observability.AttrChannelType.String(channelType),
		observability.AttrChannelID.String(channelId),
		observability.AttrServiceCount.Int(len(services)),
	)
	defer func() { observability.EndSpan(span, err) }()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	q := s.q.WithTx(tx)
	if err := q.DeleteServicesByChannel(ctx, gen.DeleteServicesByChannelParams{
		ChannelType: channelType,
		ChannelID:   channelId,
	}); err != nil {
		return fmt.Errorf("delete existing: %w", err)
	}

	for _, svc := range services {
		if err := q.UpsertService(ctx, gen.UpsertServiceParams{
			ID:                 svc.Id,
			ServiceID:          int64(svc.ServiceId),
			NetworkID:          int64(svc.NetworkId),
			TransportStreamID:  int64(svc.TransportStreamId),
			Name:               svc.Name,
			Type:               int64(svc.Type),
			LogoID:             svc.LogoId,
			LogoVersion:        svc.LogoVersion,
			LogoDownloadDataID: svc.LogoDownloadDataId,
			RemoteControlKeyID: int64(svc.RemoteControlKeyId),
			ChannelType:        channelType,
			ChannelID:          channelId,
		}); err != nil {
			return fmt.Errorf("upsert service %s: %w", svc.Id, err)
		}
		if svc.LogoId != nil && svc.LogoVersion != nil && svc.LogoDownloadDataId != nil {
			if err := q.DeleteStaleServiceLogosForService(ctx, gen.DeleteStaleServiceLogosForServiceParams{
				NetworkID:      int64(svc.NetworkId),
				ServiceID:      int64(svc.ServiceId),
				LogoID:         *svc.LogoId,
				LogoVersion:    *svc.LogoVersion,
				DownloadDataID: *svc.LogoDownloadDataId,
			}); err != nil {
				return fmt.Errorf("delete stale service logos %s: %w", svc.Id, err)
			}
		} else {
			if err := q.DeleteServiceLogosForService(ctx, gen.DeleteServiceLogosForServiceParams{
				NetworkID: int64(svc.NetworkId),
				ServiceID: int64(svc.ServiceId),
			}); err != nil {
				return fmt.Errorf("delete service logos %s: %w", svc.Id, err)
			}
		}
	}

	return tx.Commit()
}

func (s *sqliteStore) PruneChannels(ctx context.Context, active []ChannelKey) error {
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
	defer tx.Rollback()
	q := s.q.WithTx(tx)
	for key := range stale {
		if err := q.DeleteServicesByChannel(ctx, gen.DeleteServicesByChannelParams{ChannelType: key.Type, ChannelID: key.ID}); err != nil {
			return fmt.Errorf("delete stale channel %s/%s: %w", key.Type, key.ID, err)
		}
	}
	return tx.Commit()
}

type serviceRow struct {
	id                 string
	serviceID          int64
	networkID          int64
	transportStreamID  int64
	name               string
	typ                int64
	logoID             *int64
	logoVersion        *int64
	logoDownloadDataID *int64
	hasLogoData        bool
	remoteControlKeyID int64
	channelType        string
	channelID          string
	lastAttemptAt      *int64
	lastSuccessAt      *int64
	lastError          *string
}

func fromServiceRow(s serviceRow) *Service {
	result := &Service{
		Id:                 s.id,
		ServiceId:          uint16(s.serviceID),
		NetworkId:          uint16(s.networkID),
		TransportStreamId:  uint16(s.transportStreamID),
		Name:               s.name,
		Type:               uint8(s.typ),
		LogoId:             s.logoID,
		LogoVersion:        s.logoVersion,
		LogoDownloadDataId: s.logoDownloadDataID,
		HasLogoData:        s.hasLogoData,
		RemoteControlKeyId: uint8(s.remoteControlKeyID),
		ChannelType:        s.channelType,
		ChannelId:          s.channelID,
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
	return serviceRow{s.ID, s.ServiceID, s.NetworkID, s.TransportStreamID, s.Name, s.Type, s.LogoID, s.LogoVersion, s.LogoDownloadDataID, s.HasLogoData, s.RemoteControlKeyID, s.ChannelType, s.ChannelID, s.LastAttemptAt, s.LastSuccessAt, s.LastError}
}

func getServiceByIDRow(s gen.GetServiceByIDRow) serviceRow {
	return serviceRow{s.ID, s.ServiceID, s.NetworkID, s.TransportStreamID, s.Name, s.Type, s.LogoID, s.LogoVersion, s.LogoDownloadDataID, s.HasLogoData, s.RemoteControlKeyID, s.ChannelType, s.ChannelID, s.LastAttemptAt, s.LastSuccessAt, s.LastError}
}

func getServiceByItemIDRow(s gen.GetServiceByItemIDRow) serviceRow {
	return serviceRow{s.ID, s.ServiceID, s.NetworkID, s.TransportStreamID, s.Name, s.Type, s.LogoID, s.LogoVersion, s.LogoDownloadDataID, s.HasLogoData, s.RemoteControlKeyID, s.ChannelType, s.ChannelID, s.LastAttemptAt, s.LastSuccessAt, s.LastError}
}

func getServiceByNetworkServiceIDRow(s gen.GetServiceByNetworkServiceIDRow) serviceRow {
	return serviceRow{s.ID, s.ServiceID, s.NetworkID, s.TransportStreamID, s.Name, s.Type, s.LogoID, s.LogoVersion, s.LogoDownloadDataID, s.HasLogoData, s.RemoteControlKeyID, s.ChannelType, s.ChannelID, s.LastAttemptAt, s.LastSuccessAt, s.LastError}
}

func getServicesByChannelRow(s gen.GetServicesByChannelRow) serviceRow {
	return serviceRow{s.ID, s.ServiceID, s.NetworkID, s.TransportStreamID, s.Name, s.Type, s.LogoID, s.LogoVersion, s.LogoDownloadDataID, s.HasLogoData, s.RemoteControlKeyID, s.ChannelType, s.ChannelID, s.LastAttemptAt, s.LastSuccessAt, s.LastError}
}

func getServiceByChannelAndIDRow(s gen.GetServiceByChannelAndIDRow) serviceRow {
	return serviceRow{s.ID, s.ServiceID, s.NetworkID, s.TransportStreamID, s.Name, s.Type, s.LogoID, s.LogoVersion, s.LogoDownloadDataID, s.HasLogoData, s.RemoteControlKeyID, s.ChannelType, s.ChannelID, s.LastAttemptAt, s.LastSuccessAt, s.LastError}
}
