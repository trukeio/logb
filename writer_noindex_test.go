package logb

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// A writer with NoIndex keeps nothing per DATA frame, writes no INDEX frame,
// and its stream reads back whole: a live stream of a frame per record must
// not grow by an index entry a record for as long as it runs.
func TestNoIndex(t *testing.T) {
	write := func(noIndex bool) ([]byte, *Writer) {
		var buf bytes.Buffer
		w, err := NewWriter(&buf)
		if err != nil {
			t.Fatal(err)
		}
		w.NoIndex = noIndex
		s := &Schema{UUID: [16]byte{2}, Name: "mon.alive", RecordBits: 128,
			AxisKind: AxisTime, AxisMode: AxisExplicit, AxisExp: -9, AxisUnit: "s", AxisScale: TickVal(1),
			Fields: []Field{{Name: "t", BitWidth: 64, Type: TypeSint}, {Name: "v", BitOffset: 64, BitWidth: 64, Type: TypeFloat}}}
		if err := w.AddStream(s); err != nil {
			t.Fatal(err)
		}
		if err := w.BeginSegment(0); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 1000; i++ {
			if err := w.WriteData(s, TickVal(0), 0, 1, make([]byte, 16)); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return buf.Bytes(), w
	}
	read := func(b []byte) (records, indexes int) {
		r, err := NewReader(bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		r.OnFrame = func(f Frame) {
			if f.Type == FrameIndex {
				indexes++
			}
		}
		for {
			batch, err := r.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			records += int(batch.Count)
		}
		if r.Truncated {
			t.Fatal("the stream ended at damage rather than at a clean end")
		}
		return records, indexes
	}

	b, w := write(true)
	if len(w.index) != 0 || len(w.order) != 0 {
		t.Errorf("NoIndex kept %d index groups", len(w.index))
	}
	if n, idx := read(b); n != 1000 || idx != 0 {
		t.Errorf("NoIndex: %d records and %d INDEX frames, want 1000 and 0", n, idx)
	}
	b, w = write(false)
	if got := len(w.index[[16]byte{2}]); got != 1000 {
		t.Errorf("the default kept %d index entries, want one a frame, 1000", got)
	}
	if n, idx := read(b); n != 1000 || idx != 1 {
		t.Errorf("default: %d records and %d INDEX frames, want 1000 and 1", n, idx)
	}
}
