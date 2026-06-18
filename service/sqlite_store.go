package service

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/21S1298001/Mahiron5/db/gen"
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
		result[i] = fromStoregenService(svcs[i])
	}
	if err := s.attachEPGStatuses(ctx, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *sqliteStore) GetByID(ctx context.Context, id string) (*Service, error) {
	svc, err := s.q.GetServiceByID(ctx, id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	result := fromStoregenService(svc)
	if err := s.attachEPGStatuses(ctx, []*Service{result}); err != nil {
		return nil, err
	}
	return result, nil
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
		result[i] = fromStoregenService(svcs[i])
	}
	if err := s.attachEPGStatuses(ctx, result); err != nil {
		return nil, err
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

func (s *sqliteStore) attachEPGStatuses(ctx context.Context, services []*Service) error {
	if len(services) == 0 {
		return nil
	}
	statuses, err := s.q.ListEPGStatuses(ctx)
	if err != nil {
		return err
	}
	type key struct{ networkID, serviceID uint16 }
	byKey := make(map[key]*Service, len(services))
	for _, svc := range services {
		byKey[key{svc.NetworkId, svc.ServiceId}] = svc
	}
	for _, st := range statuses {
		svc, ok := byKey[key{uint16(st.NetworkID), uint16(st.ServiceID)}]
		if !ok {
			continue
		}
		svc.EPG.LastAttemptAt = st.LastAttemptAt
		svc.EPG.LastSuccessAt = st.LastSuccessAt
		if st.LastError != nil {
			svc.EPG.LastError = *st.LastError
		}
	}
	return nil
}

func nullableString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func (s *sqliteStore) ReplaceChannelServices(ctx context.Context, channelType, channelId string, services []*Service) error {
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
			RemoteControlKeyID: int64(svc.RemoteControlKeyId),
			ChannelType:        channelType,
			ChannelID:          channelId,
		}); err != nil {
			return fmt.Errorf("upsert service %s: %w", svc.Id, err)
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

func fromStoregenService(s gen.Service) *Service {
	return &Service{
		Id:                 s.ID,
		ServiceId:          uint16(s.ServiceID),
		NetworkId:          uint16(s.NetworkID),
		TransportStreamId:  uint16(s.TransportStreamID),
		Name:               s.Name,
		Type:               uint8(s.Type),
		RemoteControlKeyId: uint8(s.RemoteControlKeyID),
		ChannelType:        s.ChannelType,
		ChannelId:          s.ChannelID,
	}
}
