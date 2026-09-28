package packet

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// fixedDetector reports the same Result for every call, standing in for a
// real Detector so CheckReader's own logic can be tested independently of
// any format.
type fixedDetector struct {
	name   string
	result Result
	// matchAtLen only matches once buf reaches this length; zero means
	// never (Detect always reports the fixed result below that length).
	matchAtLen int
}

func (d fixedDetector) Name() string { return d.name }

func (d fixedDetector) Detect(buf []byte, final bool) Result {
	if d.matchAtLen > 0 && len(buf) >= d.matchAtLen {
		return Matched
	}
	if final {
		if d.result == Undecided {
			return Rejected
		}
		return d.result
	}
	return Undecided
}

func TestCheckReaderPassesThroughOnceSelfMatches(t *testing.T) {
	self := fixedDetector{name: "a", matchAtLen: 4}
	data := []byte("abcdefgh")
	r := NewCheckReader(bytes.NewReader(data), self)

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("read = %q, want %q", got, data)
	}
}

func TestCheckReaderReportsMismatchAgainstAnother(t *testing.T) {
	self := fixedDetector{name: "a", result: Rejected}
	other := fixedDetector{name: "b", matchAtLen: 1}
	r := NewCheckReader(bytes.NewReader([]byte("x")), self, other)

	_, err := io.ReadAll(r)
	var mismatch *MismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("err = %v, want *MismatchError", err)
	}
	if mismatch.Want != "a" || mismatch.Got != "b" {
		t.Fatalf("mismatch = %+v, want Want=a Got=b", mismatch)
	}
}

func TestCheckReaderReportsUnknownWhenNothingMatches(t *testing.T) {
	self := fixedDetector{name: "a", result: Rejected}
	other := fixedDetector{name: "b", result: Rejected}
	r := NewCheckReader(bytes.NewReader([]byte("x")), self, other)

	_, err := io.ReadAll(r)
	var unknown *UnknownError
	if !errors.As(err, &unknown) {
		t.Fatalf("err = %v, want *UnknownError", err)
	}
	if unknown.Want != "a" {
		t.Fatalf("unknown.Want = %q, want a", unknown.Want)
	}
}

func TestCheckReaderPropagatesReadErrorBeforeAnyByte(t *testing.T) {
	self := fixedDetector{name: "a"}
	wantErr := errors.New("boom")
	r := NewCheckReader(failingReader{wantErr}, self)

	_, err := io.ReadAll(r)
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
}

func TestCheckReaderChecksOnceThenStopsCalling(t *testing.T) {
	self := &countingDetector{fixedDetector: fixedDetector{name: "a", matchAtLen: 1}}
	r := NewCheckReader(bytes.NewReader([]byte("abc")), self)

	buf := make([]byte, 1)
	for range 3 {
		if _, err := r.Read(buf); err != nil && err != io.EOF {
			t.Fatal(err)
		}
	}
	if self.calls != 1 {
		t.Fatalf("Detect calls = %d, want 1 (only while unresolved)", self.calls)
	}
}

type countingDetector struct {
	fixedDetector
	calls int
}

func (d *countingDetector) Detect(buf []byte, final bool) Result {
	d.calls++
	return d.fixedDetector.Detect(buf, final)
}

type failingReader struct{ err error }

func (r failingReader) Read([]byte) (int, error) { return 0, r.err }
