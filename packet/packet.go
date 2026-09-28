// Package packet tells one packet format (MPEG-2 TS, ISDB-S3 TLV, and any
// later one) apart from another before a byte reaches the wrong parser. Each
// format's own package (ts, mmt) owns the Detector that recognizes it, so a
// later format only means adding one Detector.
package packet

import (
	"fmt"
	"io"
)

// Result is one Detector's read on the bytes seen so far.
type Result int

const (
	// Undecided means more of the stream is needed; never returned when final.
	Undecided Result = iota
	// Matched means the bytes seen so far start this packet format.
	Matched
	// Rejected means the bytes seen so far cannot start this packet format.
	Rejected
)

// Detector recognizes one packet format at the start of a byte stream.
type Detector interface {
	// Name identifies the packet format in errors, such as "ts" or "tlv".
	Name() string
	// Detect classifies buf, the bytes read so far. final means no more
	// bytes are coming, and Detect must then return Matched or Rejected. A
	// tuner or recording may start mid packet, so a Detector generally looks
	// for a short run of its own packets rather than trusting the first byte.
	Detect(buf []byte, final bool) Result
}

// MismatchError is returned when the stream carries a different, recognized
// packet format than the one it was checked against.
type MismatchError struct {
	Want, Got string
}

func (e *MismatchError) Error() string {
	return fmt.Sprintf("stream is %s, want %s", e.Got, e.Want)
}

// UnknownError is returned when no registered packet format recognized
// the stream.
type UnknownError struct {
	Want string
}

func (e *UnknownError) Error() string {
	return fmt.Sprintf("stream matches no known packet format, want %s", e.Want)
}

// probeLimit bounds how much of the stream is read while undecided; it must
// fit several of the largest packets of every registered format (an
// ISDB-S3 TLV packet can carry up to 65535 bytes) so a wrong-format stream
// is identified rather than merely rejected as unknown.
const probeLimit = 256 << 10

// NewCheckReader wraps r so the first bytes read decide whether the stream
// starts with self's format before any byte reaches the caller. Once self
// matches, later reads pass the probed and remaining bytes through
// unchanged; otherwise Read returns a *MismatchError or *UnknownError.
func NewCheckReader(r io.Reader, self Detector, others ...Detector) io.Reader {
	return &checkReader{r: r, self: self, others: others}
}

type checkReader struct {
	r       io.Reader
	self    Detector
	others  []Detector
	checked bool
	probe   []byte
}

func (c *checkReader) Read(p []byte) (int, error) {
	if !c.checked {
		if err := c.check(); err != nil {
			return 0, err
		}
	}
	if len(c.probe) > 0 {
		n := copy(p, c.probe)
		c.probe = c.probe[n:]
		return n, nil
	}
	return c.r.Read(p)
}

func (c *checkReader) check() error {
	buf := make([]byte, 0, 64<<10)
	var readErr error
	for {
		if len(buf) == cap(buf) {
			buf = append(buf, 0)[:len(buf)]
		}
		n, err := c.r.Read(buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+n]
		readErr = err
		final := err != nil || len(buf) >= probeLimit
		if c.self.Detect(buf, final) == Matched {
			c.checked = true
			c.probe = buf
			return nil
		}
		if final {
			if len(buf) == 0 {
				return readErr
			}
			return c.mismatchOrUnknown(buf)
		}
	}
}

// mismatchOrUnknown is called once self has been ruled out (or never
// matched by the probe limit) and reports which other packet format, if
// any, recognizes the same bytes.
func (c *checkReader) mismatchOrUnknown(buf []byte) error {
	for _, other := range c.others {
		if other.Detect(buf, true) == Matched {
			return &MismatchError{Want: c.self.Name(), Got: other.Name()}
		}
	}
	return &UnknownError{Want: c.self.Name()}
}
