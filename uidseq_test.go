package imapmaildir

import (
	"testing"

	"github.com/emersion/go-imap"
)

func TestSeqToUid(t *testing.T) {
	uidMap := []uint32{2, 4, 6, 7, 8}
	test := func(seq, res imap.Seq, fail bool) {
		t.Helper()

		actualRes, err := seqToUid(uidMap, seq)
		if (err != nil) != fail {
			t.Errorf("%v => %v; fail: %v; err: %v", seq, res, fail, err)
			return
		}
		if err != nil {
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

	test(imap.Seq{1, 0}, imap.Seq{2, 8}, false)
	test(imap.Seq{0, 0}, imap.Seq{8, 8}, false)
	test(imap.Seq{1, 7}, imap.Seq{2, 8}, false)
	test(imap.Seq{1, 5}, imap.Seq{2, 8}, false)
	test(imap.Seq{1, 1}, imap.Seq{2, 2}, false)
	test(imap.Seq{5, 5}, imap.Seq{8, 8}, false)
	test(imap.Seq{2, 2}, imap.Seq{4, 4}, false)
	test(imap.Seq{2, 4}, imap.Seq{4, 7}, false)
	test(imap.Seq{6, 0}, uselessSeq, true)
	test(imap.Seq{6, 6}, uselessSeq, true)

	uidMap = []uint32{}
	test(imap.Seq{1, 0}, uselessSeq, true)

	uidMap = []uint32{4}
	test(imap.Seq{1, 0}, imap.Seq{4, 4}, false)

	uidMap = []uint32{2, 4, 0, 7, 8}
	test(imap.Seq{2, 3}, imap.Seq{4, 4}, false)
	test(imap.Seq{3, 3}, uselessSeq, true)
}

func TestUidToSeq(t *testing.T) {
	uidMap := []uint32{2, 4, 6, 7, 8}
	test := func(seq, res imap.Seq, fail bool) {
		t.Helper()

		actualRes, err := uidToSeq(uidMap, seq)
		if (err != nil) != fail {
			t.Errorf("%v => %v; fail: %v; err: %v", seq, res, fail, err)
			return
		}
		if err != nil {
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

	test(imap.Seq{1, 0}, imap.Seq{1, 5}, false)
	test(imap.Seq{1, 8}, imap.Seq{1, 5}, false)
	test(imap.Seq{2, 8}, imap.Seq{1, 5}, false)
	test(imap.Seq{2, 10}, imap.Seq{1, 5}, false)
	test(imap.Seq{2, 2}, imap.Seq{1, 1}, false)
	test(imap.Seq{0, 0}, imap.Seq{5, 5}, false)
	test(imap.Seq{8, 8}, imap.Seq{5, 5}, false)
	test(imap.Seq{3, 5}, imap.Seq{2, 2}, false)
	test(imap.Seq{9, 10}, uselessSeq, true)
	test(imap.Seq{1, 1}, uselessSeq, true)

	uidMap = []uint32{}
	test(imap.Seq{1, 0}, uselessSeq, true)
	uidMap = []uint32{4}
	test(imap.Seq{4, 4}, imap.Seq{1, 1}, false)
}
