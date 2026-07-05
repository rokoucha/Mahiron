package local

import (
	"context"
	"encoding/binary"
	"log/slog"

	"github.com/21S1298001/mahiron/ts"
)

// sectionQueueSize bounds the queue between section observation and the
// asynchronous EIT/logo updater pump.
const sectionQueueSize = 64

// carouselQueueSize bounds the queue for DSM-CC data carousel sections
// (BS common logo download). It is sized separately from sectionQueue
// because a data carousel can emit far more sections per second than
// EIT/CDT/SDTT, and must not be allowed to starve them.
const carouselQueueSize = 256

// EITSectionUpdater persists EIT sections observed on the stream.
type EITSectionUpdater interface {
	UpsertEIT(ctx context.Context, eit *ts.EIT) error
}

// LogoUpdater persists logo images and related announcements observed on the
// stream.
type LogoUpdater interface {
	UpsertLogoImage(context.Context, *ts.LogoImage) error
	UpsertCommonLogoImage(context.Context, ts.CommonLogoImage) error
	UpsertCommonDataAnnouncement(context.Context, ts.CommonDataAnnouncement, string, string) error
}

type eitPFSectionKey struct {
	tableID           byte
	serviceID         uint16
	transportStreamID uint16
	originalNetworkID uint16
	sectionNumber     byte
}

func (s *Session) observeSection(section ts.Section) {
	switch section.TableID() {
	case ts.TableIDDSMCCDII, ts.TableIDDSMCCDDB:
		select {
		case s.carouselQueue <- section:
		default:
			slog.Warn("TS carousel updater overflow", "type", s.typ, "channel", s.channel)
		}
		return
	}
	if !ts.IsEITPF(section.TableID()) && section.TableID() != ts.TableIDCDT && section.TableID() != ts.TableIDSDTT {
		return
	}
	key, fingerprint, coalesce := eitPFSectionFingerprint(section)
	if coalesce && !s.reserveEITPFSection(key, fingerprint) {
		return
	}
	select {
	case s.sectionQueue <- section:
	default:
		if coalesce {
			s.releaseEITPFSection(key, fingerprint)
		}
		slog.Warn("TS section updater overflow", "type", s.typ, "channel", s.channel)
	}
}

func (s *Session) observePIDSection(section ts.PIDSection) {
	if s.dataBroadcast != nil {
		s.dataBroadcast.Observe(section)
	}
}

func (s *Session) runSectionUpdates(ctx context.Context) {
	if s.sectionDone != nil {
		defer close(s.sectionDone)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case section := <-s.sectionQueue:
			s.updateSection(ctx, section)
		case section := <-s.carouselQueue:
			s.updateSection(ctx, section)
		}
	}
}

func (s *Session) updateSection(ctx context.Context, section ts.Section) {
	if ts.IsEITPF(section.TableID()) && s.eitUpdater != nil {
		key, fingerprint, coalesced := eitPFSectionFingerprint(section)
		eit, err := ts.ParseEIT(section)
		if err != nil {
			if coalesced {
				s.releaseEITPFSection(key, fingerprint)
			}
		} else if err := s.eitUpdater.UpsertEIT(ctx, eit); err != nil {
			if coalesced {
				s.releaseEITPFSection(key, fingerprint)
			}
			slog.Error("failed to update EITPF", "type", s.typ, "channel", s.channel, "err", err)
		}
	}
	if section.TableID() == ts.TableIDCDT && s.logoUpdater != nil {
		if cdt, err := ts.ParseCDT(section); err == nil {
			if image, err := ts.ParseCDTLogoImage(cdt); err == nil {
				if err := s.logoUpdater.UpsertLogoImage(ctx, image); err != nil {
					slog.Error("failed to update logo", "type", s.typ, "channel", s.channel, "err", err)
				}
			}
		}
	}
	if section.TableID() == ts.TableIDSDTT && s.logoUpdater != nil {
		announcements, err := ts.ParseSDTTCommonDataAnnouncements(section)
		if err != nil {
			slog.Error("failed to parse SDTT common data announcement", "type", s.typ, "channel", s.channel, "err", err)
		}
		for _, announcement := range announcements {
			if err := s.logoUpdater.UpsertCommonDataAnnouncement(ctx, announcement, s.typ, s.channel); err != nil {
				slog.Error("failed to update SDTT common data announcement", "type", s.typ, "channel", s.channel, "err", err)
			}
		}
	}
	if section.TableID() == ts.TableIDDSMCCDII && s.logoUpdater != nil {
		if dii, err := ts.ParseDSMCCDII(section); err == nil {
			s.logoCarousel.ObserveDII(dii)
		}
	}
	if section.TableID() == ts.TableIDDSMCCDDB && s.logoUpdater != nil {
		if ddb, err := ts.ParseDSMCCDDB(section); err == nil {
			images, err := s.logoCarousel.ObserveDDB(ddb)
			if err != nil {
				slog.Error("failed to parse common logo", "type", s.typ, "channel", s.channel, "err", err)
				return
			}
			for _, image := range images {
				if err := s.logoUpdater.UpsertCommonLogoImage(ctx, image); err != nil {
					slog.Error("failed to update common logo", "type", s.typ, "channel", s.channel, "err", err)
				}
			}
		}
	}
}

func eitPFSectionFingerprint(section ts.Section) (eitPFSectionKey, uint32, bool) {
	if len(section) < 14 || !ts.IsEITPF(section.TableID()) {
		return eitPFSectionKey{}, 0, false
	}
	total := section.TotalLength()
	if total < 4 || total > len(section) {
		return eitPFSectionKey{}, 0, false
	}
	return eitPFSectionKey{
		tableID:           section.TableID(),
		serviceID:         binary.BigEndian.Uint16(section[3:5]),
		transportStreamID: binary.BigEndian.Uint16(section[8:10]),
		originalNetworkID: binary.BigEndian.Uint16(section[10:12]),
		sectionNumber:     section[6],
	}, binary.BigEndian.Uint32(section[total-4 : total]), true
}

func (s *Session) reserveEITPFSection(key eitPFSectionKey, fingerprint uint32) bool {
	s.sectionUpdateMu.Lock()
	defer s.sectionUpdateMu.Unlock()
	if s.eitPFFingerprints == nil {
		s.eitPFFingerprints = make(map[eitPFSectionKey]uint32)
	}
	if current, ok := s.eitPFFingerprints[key]; ok && current == fingerprint {
		return false
	}
	s.eitPFFingerprints[key] = fingerprint
	return true
}

func (s *Session) releaseEITPFSection(key eitPFSectionKey, fingerprint uint32) {
	s.sectionUpdateMu.Lock()
	defer s.sectionUpdateMu.Unlock()
	if current, ok := s.eitPFFingerprints[key]; ok && current == fingerprint {
		delete(s.eitPFFingerprints, key)
	}
}
