package tlv

import (
	"context"
	"encoding/binary"
	"log/slog"

	"github.com/21S1298001/mahiron/mmt"
)

// sectionKey identifies a repeatedly sent section, so a section whose CRC
// did not change since the last update is not processed again.
type sectionKey struct {
	tableID          byte
	tableIDExtension uint16
	sectionNumber    byte
}

// observeSignal runs on the raw engine for every signal, so it must stay
// short: it updates the codec table directly and queues sections for the
// update worker.
func (s *Session) observeSignal(sig signal) {
	if len(sig.Table) > 0 && sig.Table.TableID() == mmt.TableIDMPT {
		s.observeCodecs(sig.Table)
		return
	}
	if sig.TLVSI || len(sig.Section) < 8 {
		return
	}
	switch id := sig.Section.TableID(); {
	case mmt.IsMHEITPF(id):
		if s.eventUpdater == nil {
			return
		}
	case id == mmt.TableIDMHSDTActual, id == mmt.TableIDMHSDTOther, id == mmt.TableIDMHCDT:
		if s.logoUpdater == nil {
			return
		}
	default:
		return
	}
	key, fingerprint := sectionFingerprint(sig.Section)
	if !s.reserveSection(key, fingerprint) {
		return
	}
	select {
	case s.updates <- sig:
	default:
		s.releaseSection(key, fingerprint)
		slog.Warn("TLV section updater overflow", "type", s.typ, "channel", s.channel)
	}
}

func sectionFingerprint(section mmt.Section) (sectionKey, uint32) {
	total := min(section.TotalLength(), len(section))
	return sectionKey{
		tableID:          section.TableID(),
		tableIDExtension: binary.BigEndian.Uint16(section[3:5]),
		sectionNumber:    section[6],
	}, binary.BigEndian.Uint32(section[total-4 : total])
}

func (s *Session) reserveSection(key sectionKey, fingerprint uint32) bool {
	s.fingerprintMu.Lock()
	defer s.fingerprintMu.Unlock()
	if s.fingerprints == nil {
		s.fingerprints = map[sectionKey]uint32{}
	}
	if current, ok := s.fingerprints[key]; ok && current == fingerprint {
		return false
	}
	s.fingerprints[key] = fingerprint
	return true
}

func (s *Session) releaseSection(key sectionKey, fingerprint uint32) {
	s.fingerprintMu.Lock()
	defer s.fingerprintMu.Unlock()
	if current, ok := s.fingerprints[key]; ok && current == fingerprint {
		delete(s.fingerprints, key)
	}
}

func (s *Session) startUpdatesLocked() {
	if s.updateCancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.updateCancel = cancel
	done := make(chan struct{})
	s.updateDone = done
	go s.runUpdates(ctx, done)
}

func (s *Session) stopUpdates() {
	s.mu.Lock()
	cancel := s.updateCancel
	done := s.updateDone
	s.updateCancel = nil
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (s *Session) runUpdates(ctx context.Context, done chan struct{}) {
	defer close(done)
	for {
		select {
		case <-ctx.Done():
			return
		case sig := <-s.updates:
			s.update(ctx, sig)
		}
	}
}

// update persists the present/following events and the logos a section
// completes.
func (s *Session) update(ctx context.Context, sig signal) {
	if mmt.IsMHEITPF(sig.Section.TableID()) {
		key, fingerprint := sectionFingerprint(sig.Section)
		eit, err := mmt.ParseMHEIT(sig.Section)
		if err != nil {
			s.releaseSection(key, fingerprint)
			return
		}
		if err := s.eventUpdater.UpsertEvents(ctx, s.fillCodecs(eventsFromMHEIT(eit))); err != nil {
			s.releaseSection(key, fingerprint)
			slog.Error("failed to update MH-EIT p/f", "type", s.typ, "channel", s.channel, "err", err)
		}
		return
	}
	for _, logo := range s.logos.Observe(sig) {
		if err := s.logoUpdater.UpsertLogoImage(ctx, logo); err != nil {
			slog.Error("failed to update logo", "type", s.typ, "channel", s.channel, "err", err)
		}
	}
}
