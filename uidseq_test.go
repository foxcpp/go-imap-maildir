package imapmaildir

import (
	"testing"

	"github.com/emersion/go-imap"
)

func TestSeqToUid(t *testing.T) {
	uidMap := []uint32{2, 4, 6, 7, 8}
	test := func(seq, res imap.Seq, fail bool) {
		t.Helper()

		actualRes, ok := seqToUid(uidMap, seq)
		if !ok != fail {
			t.Errorf("%v => %v; fail: %v; ok: %v", seq, res, fail, ok)
			return
		}
		if !ok {
			return
		}
		if res.Start != actualRes.Start {
			t.Errorf("%v => %v; got %v", seq, res, actualRes)
			return
		}
		if res.Stop != actualRes.Stop {
			t.Errorf("%v => %v; got %v", seq, res, actualRes)
		}
	}

	test(imap.Seq{Start: 1}, imap.Seq{Start: 2, Stop: 8}, false)
	test(imap.Seq{}, imap.Seq{Start: 8, Stop: 8}, false)
	test(imap.Seq{Start: 1, Stop: 7}, imap.Seq{Start: 2, Stop: 8}, false)
	test(imap.Seq{Start: 1, Stop: 5}, imap.Seq{Start: 2, Stop: 8}, false)
	test(imap.Seq{Start: 1, Stop: 1}, imap.Seq{Start: 2, Stop: 2}, false)
	test(imap.Seq{Start: 5, Stop: 5}, imap.Seq{Start: 8, Stop: 8}, false)
	test(imap.Seq{Start: 2, Stop: 2}, imap.Seq{Start: 4, Stop: 4}, false)
	test(imap.Seq{Start: 2, Stop: 4}, imap.Seq{Start: 4, Stop: 7}, false)
	test(imap.Seq{Start: 6}, uselessSeq, true)
	test(imap.Seq{Start: 6, Stop: 6}, uselessSeq, true)

	uidMap = []uint32{}
	test(imap.Seq{Start: 1}, uselessSeq, true)

	uidMap = []uint32{4}
	test(imap.Seq{Start: 1}, imap.Seq{Start: 4, Stop: 4}, false)

	uidMap = []uint32{2, 4, 0, 7, 8}
	test(imap.Seq{Start: 2, Stop: 3}, imap.Seq{Start: 4, Stop: 4}, false)
	test(imap.Seq{Start: 3, Stop: 3}, uselessSeq, true)
}

func TestUidToSeq(t *testing.T) {
	uidMap := []uint32{2, 4, 6, 7, 8}
	test := func(seq, res imap.Seq, fail bool) {
		t.Helper()

		actualRes, ok := uidToSeq(uidMap, seq)
		if !ok != fail {
			t.Errorf("%v => %v; fail: %v; ok: %v", seq, res, fail, ok)
			return
		}
		if !ok {
			return
		}
		if res.Start != actualRes.Start {
			t.Errorf("%v => %v; got %v", seq, res, actualRes)
			return
		}
		if res.Stop != actualRes.Stop {
			t.Errorf("%v => %v; got %v", seq, res, actualRes)
		}
	}

	test(imap.Seq{Start: 1}, imap.Seq{Start: 1, Stop: 5}, false)
	test(imap.Seq{Start: 1, Stop: 8}, imap.Seq{Start: 1, Stop: 5}, false)
	test(imap.Seq{Start: 2, Stop: 8}, imap.Seq{Start: 1, Stop: 5}, false)
	test(imap.Seq{Start: 2, Stop: 10}, imap.Seq{Start: 1, Stop: 5}, false)
	test(imap.Seq{Start: 2, Stop: 2}, imap.Seq{Start: 1, Stop: 1}, false)
	test(imap.Seq{}, imap.Seq{Start: 5, Stop: 5}, false)
	test(imap.Seq{Start: 8, Stop: 8}, imap.Seq{Start: 5, Stop: 5}, false)
	test(imap.Seq{Start: 3, Stop: 5}, imap.Seq{Start: 2, Stop: 2}, false)
	test(imap.Seq{Start: 9, Stop: 10}, uselessSeq, true)
	test(imap.Seq{Start: 9, Stop: 5}, uselessSeq, true)
	test(imap.Seq{Start: 1, Stop: 1}, uselessSeq, true)

	uidMap = []uint32{}
	test(imap.Seq{Start: 1}, uselessSeq, true)
	uidMap = []uint32{4}
	test(imap.Seq{Start: 4, Stop: 4}, imap.Seq{Start: 1, Stop: 1}, false)

	uidMap = []uint32{0, 2, 0, 4, 5, 6, 7, 8}
	test(imap.Seq{Start: 2, Stop: 2}, imap.Seq{Start: 2, Stop: 2}, false)
	test(imap.Seq{Start: 4, Stop: 4}, imap.Seq{Start: 4, Stop: 4}, false)
}
