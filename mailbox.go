package imapmaildir

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/asdine/storm/v3"
	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/backend"
	"github.com/emersion/go-imap/backend/backendutil"
	"github.com/emersion/go-maildir"
	"go.etcd.io/bbolt"
)

type Mailbox struct {
	b *Backend

	// DB handle, see dbs comment in Backend - it may be not only ours handle.
	// Also it is nil for Mailbox'es created by ListMailboxes.
	handle *storm.DB
	dir    maildir.Dir

	username string

	// Full mailbox name.
	name string

	// Filesystem path to the mailbox directory. Not guaranteed to be absolute
	// or relative.
	path string

	sharedHandle *mailboxHandle
	thisConn     backend.Conn

	uidMap             []uint32
	pendingUIDs        []uint32
	pendingFlags       []flagUpdate
	stopUpdateHandling chan struct{}
}

type mboxData struct {
	Dummy int `storm:"id"`

	UidValidity uint32
	UidNext     uint32
	MsgsCount   uint32
}

type message struct {
	UID uint32 `storm:"id,increment"`

	// This structure contains minimal information about message
	// because it is often range-scanned by various operations.

	// XXX Struct might get overriden by flag-related operations
	// update these to not do so if more fields are added there.

	Unseen  bool
	Deleted bool
}

func (m message) key() string {
	return "imap-" + strconv.FormatUint(uint64(m.UID), 10)
}

type messageFlags struct {
	UID   uint32 `storm:"id,increment"`
	Flags []string
}

type messageInfo struct {
	UID          uint32 `storm:"id,increment"`
	RFC822Size   uint32
	InternalDate time.Time
}

type messageCache struct {
	UID           uint32 `storm:"id"`
	Envelope      *imap.Envelope
	BodyStructure *imap.BodyStructure
}

func (m *Mailbox) Name() string {
	return m.name
}

func (m *Mailbox) Info() (*imap.MailboxInfo, error) {
	// TODO: CHILDREN extension
	// TODO: SPECIAL-USE extension

	// This function should complete without using DB as it will be called
	// by LIST handler in go-imap and ListMailboxes does not initialize it for
	// performance reasons.

	info := &imap.MailboxInfo{
		Attributes: nil,
		Delimiter:  HierarchySep,
		Name:       m.name,
	}
	_, err := os.Stat(filepath.Join(m.path, "cur"))
	if err != nil {
		if os.IsNotExist(err) {
			info.Attributes = append(info.Attributes, imap.NoSelectAttr)
		} else {
			return nil, errors.New("I/O error")
		}
	}
	if strings.Count(m.name, HierarchySep) == MaxMboxNesting {
		info.Attributes = append(info.Attributes, imap.NoInferiorsAttr)
	}

	return info, nil
}

func (m *Mailbox) error(descr string, cause error, args ...interface{}) {
	if cause == nil {
		m.b.Log.Printf("mailbox %s %s: %s", m.username, m.name, fmt.Sprintf(descr, args...))
	} else {
		m.b.Log.Printf("mailbox %s %s: %s: %v", m.username, m.name, fmt.Sprintf(descr, args...), cause)
	}
}

func (m *Mailbox) Status(items []imap.StatusItem) (*imap.MailboxStatus, error) {
	_, status, err := m.status(items, false)
	return status, err
}

func (m *Mailbox) status(items []imap.StatusItem, collectUids bool) ([]uint32, *imap.MailboxStatus, error) {
	status := imap.NewMailboxStatus(m.name, items)

	status.Flags = []string{
		imap.SeenFlag, imap.AnsweredFlag, imap.FlaggedFlag,
		imap.DeletedFlag, imap.DraftFlag,
	}
	status.PermanentFlags = []string{
		imap.SeenFlag, imap.AnsweredFlag, imap.FlaggedFlag,
		imap.DeletedFlag, imap.DraftFlag,
	}
	// TODO: Report used flags (cache them?)

	var mboxMeta mboxData

	if err := m.handle.One("Dummy", 1, &mboxMeta); err != nil {
		m.error("Status: fetch mboxData", err)
		return nil, nil, errors.New("I/O error")
	}

	var (
		needMsgCount bool
		needRecent   bool
		needUnseen   bool
		msgCounter   uint32
	)
	for _, item := range items {
		switch item {
		case imap.StatusMessages:
			needMsgCount = true
		case imap.StatusRecent:
			// TODO: Consider using "new" directory for implementing Recent
			needRecent = true
		case imap.StatusUidNext:
			status.UidNext = mboxMeta.UidNext
		case imap.StatusUidValidity:
			status.UidValidity = mboxMeta.UidValidity
		case imap.StatusUnseen:
			needUnseen = true
		default:
			return nil, nil, fmt.Errorf("unknown status item: %s", item)
		}
	}

	var uids []uint32

	if collectUids {
		size := mboxMeta.MsgsCount
		if size > 10000 {
			size = 10000
		}
		uids = make([]uint32, 0, size)
	}

	q := m.handle.Select().OrderBy("UID").Limit(10000)
	err := q.Each(new(message), func(rec interface{}) error {
		msg := rec.(*message)
		msgCounter++
		if needUnseen && msg.Unseen {
			status.Unseen++
		}
		if status.UnseenSeqNum == 0 && msg.Unseen {
			status.UnseenSeqNum = msgCounter + 1
		}

		if collectUids {
			uids = append(uids, msg.UID)
		}

		return nil
	})
	if err != nil && err != storm.ErrNotFound {
		m.error("Status", err)
		return nil, nil, errors.New("I/O error")
	}
	if needMsgCount {
		status.Messages = msgCounter
	}
	if needRecent {
		status.Recent = msgCounter
	}

	if needMsgCount && msgCounter != mboxMeta.MsgsCount {
		m.b.Log.Printf("mailbox %s/%s: BUG: cached message count de-sync, actual: %d, cache: %d",
			m.username, m.name, msgCounter, mboxMeta.MsgsCount)

		mboxMeta.MsgsCount = msgCounter
		if err := m.handle.Set("Dummy", 1, &mboxMeta); err != nil {
			m.error("Status: fix-up message count", err)
		}
	}

	return uids, status, nil
}

func (m *Mailbox) Check() error {
	return m.synchronize(true, true)
}

func (m *Mailbox) Poll() error {
	return m.synchronize(true, false)
}

func (m *Mailbox) synchronize(expunges, checkDB bool) error {
	return m.handle.Bolt.Update(func(btx *bbolt.Tx) error {
		tx := m.handle.WithTransaction(btx)

		var mboxMeta mboxData
		if err := tx.One("Dummy", 1, &mboxMeta); err != nil {
			m.error("synchronize: fetch mboxData", err)
			return errors.New("I/O error")
		}

		if expunges {
			expunged := make([]uint32, 0, 16)
			newMap := m.uidMap[:0]
			for i, uid := range m.uidMap {
				if uid == 0 {
					expunged = append(expunged, uint32(i+1))
				} else {
					newMap = append(newMap, uid)
				}
			}
			m.uidMap = newMap

			for i := len(expunged) - 1; i >= 0; i-- {
				m.thisConn.SendUpdate(&backend.ExpungeUpdate{
					SeqNum: expunged[i],
				})
			}
		}

		added := false
		if checkDB {
			var msgs []message
			if err := tx.Range("UID", m.uidMap[len(m.uidMap)-1]+1, math.MaxUint32, &msgs, storm.Limit(1000)); err != nil {
				m.error("synchronize: extra UID select failed (last UID = %v)", err, m.uidMap[len(m.uidMap)-1])
				return nil
			}
			for _, msg := range msgs {
				added = true
				m.uidMap = append(m.uidMap, msg.UID)
			}
		} else {
			added = len(m.pendingUIDs) != 0
			m.uidMap = append(m.uidMap, m.pendingUIDs...)
			m.pendingUIDs = m.pendingUIDs[:0]
		}

		if added {
			status := imap.NewMailboxStatus("", []imap.StatusItem{imap.StatusMessages})
			status.Messages = uint32(len(m.uidMap))
			m.thisConn.SendUpdate(&backend.MailboxUpdate{
				MailboxStatus: status,
			})
		}

		for _, upd := range m.pendingFlags {
			seq, ok := m.uidAsSeq(upd.uid)
			if !ok {
				m.error("synchronize: BUG: uidToSeq failed for resolved seqset (msg ID = %v)", nil, upd.uid)
			}
			updMsg := imap.NewMessage(seq, []imap.FetchItem{imap.FetchFlags})
			updMsg.Flags = upd.newFlags
			m.thisConn.SendUpdate(&backend.MessageUpdate{
				Message: updMsg,
			})
		}
		m.pendingFlags = m.pendingFlags[:0]

		return nil
	})
}

func (m *Mailbox) ListMessages(uid bool, seqset *imap.SeqSet, items []imap.FetchItem, ch chan<- *imap.Message) error {
	defer close(ch)

	errored := false
	hasResults := false

	err := m.handle.Bolt.Update(func(btx *bbolt.Tx) error {
		tx := m.handle.WithTransaction(btx)

		seqset, err := m.resolveSeqSet(uid, *seqset)
		if err != nil {
			return err
		}

		return tx.Select().OrderBy("UID").Each(new(message), func(rec interface{}) error {
			msg := rec.(*message)

			if !seqset.Contains(msg.UID) {
				return nil
			}

			seq, ok := m.uidAsSeq(msg.UID)
			if !ok {
				m.error("ListMessages: BUG: uidToSeq failed for resolved seqset (uid %v)", nil, msg.UID)
				errored = true
				return nil
			}

			m.b.Debug.Println("ListMessages: fetching", items, "for", msg.UID)
			if err := m.fetch(ch, seq, *msg, items); err != nil {
				m.error("fetch", err)
				errored = true
				return nil
			}
			hasResults = true

			return nil
		})
	})
	if err != nil {
		if err == storm.ErrNotFound {
			if uid {
				return nil
			}
			return errors.New("No messages")
		}
		m.error("I/O error", err)
		return err
	}

	m.synchronize(uid, false)

	if !uid && !hasResults {
		return errors.New("No messages matched")
	}

	if errored {
		return errors.New("Server-side error occured, partial results returned")
	}
	return nil
}

func (m *Mailbox) SearchMessages(uid bool, criteria *imap.SearchCriteria) ([]uint32, error) {
	return nil, errors.New("imapmaildir: not implemented")
}

func (m *Mailbox) temporaryMsgPath() string {
	ts := strconv.FormatInt(time.Now().UnixNano(), 16)

	return filepath.Join(m.path, "tmp", ts+":2,")
}

func (m *Mailbox) CreateMessage(flags []string, date time.Time, body imap.Literal) error {
	hasRecent := false
	for _, f := range flags {
		if f == imap.RecentFlag {
			hasRecent = true
		}
	}
	if !hasRecent {
		flags = append(flags, imap.RecentFlag)
	}

	// Save message outside of transaction to reduce locking contention.
	tmpPath := m.temporaryMsgPath()
	// Files are read-only to help enforce the IMAP immutability requirement.
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0440)
	if err != nil {
		m.error("CreateMessage", err)
		return errors.New("I/O error, try again later")
	}
	defer f.Close()

	delTemp := func() {
		if err := os.Remove(tmpPath); err != nil {
			m.error("CreateMessage: rollback failed: temp del", err)
		}
	}

	// If implemented naively - this might change after io.Copy
	rfc822Size := body.Len()

	if _, err := io.Copy(f, body); err != nil {
		m.error("CreateMessage: copy", err)
		delTemp()
		return errors.New("I/O error, try again later")
	}

	if err := f.Sync(); err != nil {
		m.error("CreateMessage: copy", err)
		delTemp()
		return errors.New("I/O error, try again later")
	}

	msg := message{
		Unseen: true,
	}
	var info messageInfo
	err = m.handle.Bolt.Update(func(btx *bbolt.Tx) error {
		tx := m.handle.WithTransaction(btx)

		if err := tx.Save(&msg); err != nil {
			return fmt.Errorf("CreateMesage: inital save: %w", err)
		}

		info = messageInfo{
			UID:          msg.UID, // magically appears there after Save
			RFC822Size:   uint32(rfc822Size),
			InternalDate: date,
		}
		if err := tx.Save(&info); err != nil {
			return fmt.Errorf("CreateMessage %d: info save: %w", msg.UID, err)
		}
		mFlags := messageFlags{
			UID:   msg.UID,
			Flags: flags,
		}
		if err := tx.Save(&mFlags); err != nil {
			return fmt.Errorf("CreateMessage %d: flags save: %w", msg.UID, err)
		}
		cache := messageCache{UID: msg.UID}
		if err := tx.Save(&cache); err != nil {
			return fmt.Errorf("CreateMessage %d: cache save: %w", msg.UID, err)
		}

		// XXX: Ignore "new" folder for now as it requires more complicated handling.
		if err := os.Rename(tmpPath, filepath.Join(m.path, "cur", msg.key()+":2,")); err != nil {
			return fmt.Errorf("CreateMessage %d: %w", msg.UID, err)
		}

		var mboxMeta mboxData
		if err := tx.One("Dummy", 1, &mboxMeta); err != nil {
			return fmt.Errorf("CreateMessage %d: load mboxData: %w", msg.UID, err)
		}
		mboxMeta.MsgsCount++
		mboxMeta.UidNext = msg.UID + 1
		if err := tx.Save(&mboxMeta); err != nil {
			// TODO: Perhaps this should not fail and we can get away by having Status fix it?
			return fmt.Errorf("CreateMessage %d: save mboxData: %w", msg.UID, err)
		}

		m.b.Debug.Printf("CreateMessage: written UID %d as maildir key %s to mbox %s/%s", msg.UID, msg.key(), m.username, m.name)
		for range m.sharedHandle.conns {
			m.sharedHandle.created <- msg.UID
		}

		return nil
	})
	if err != nil {
		m.error("CreateMessage %d: update", err, msg.UID)
		delTemp()
		return errors.New("I/O error, try again later")
	}

	m.synchronize(false, false)

	return nil
}

func (m *Mailbox) UpdateMessagesFlags(uid bool, seqset *imap.SeqSet, operation imap.FlagsOp, silent bool, flags []string) error {
	var (
		mFlags  messageFlags
		errored = false
	)

	hasChanges := false

	err := m.handle.Bolt.Update(func(btx *bbolt.Tx) error {
		tx := m.handle.WithTransaction(btx)

		seqset, err := m.resolveSeqSet(uid, *seqset)
		if err != nil {
			return err
		}

		q := tx.Select().OrderBy("UID")
		// TODO: Avoid iterating all messages for UID queries.
		return q.Each(new(message), func(rec interface{}) error {
			msg := rec.(*message)

			if !seqset.Contains(msg.UID) {
				return nil
			}

			if err := tx.One("UID", msg.UID, &mFlags); err != nil {
				m.error("UpdateMessagesFlags: fetch", err)
				errored = true
				return nil
			}

			m.b.Debug.Println("UpdateMessageFlags: updating flags", msg.UID, mFlags.Flags, "op", operation, flags)
			mFlags.Flags = backendutil.UpdateFlags(mFlags.Flags, operation, flags)
			if err := tx.Save(&mFlags); err != nil {
				m.error("UpdateMessagesFlags: save", err)
				errored = true
				return nil
			}

			hasSeen := false
			hasDeleted := false
			for _, f := range mFlags.Flags {
				if f == imap.SeenFlag {
					hasSeen = true
				}
				if f == imap.DeletedFlag {
					hasDeleted = true
				}
			}
			msg.Unseen = !hasSeen
			msg.Deleted = hasDeleted
			if err := tx.Save(&msg); err != nil {
				m.error("UpdateMessagesFlags: save Unseen/Deleted flag failed", err)
			}

			hasChanges = true

			upd := flagUpdate{
				uid: msg.UID, newFlags: mFlags.Flags,
			}
			if silent {
				upd.silentFor = m
			}
			for range m.sharedHandle.conns {
				m.sharedHandle.flags <- upd
			}

			return nil
		})
	})
	if err != nil {
		if err == storm.ErrNotFound {
			if uid {
				return nil
			}
			return errors.New("No messages")
		}
		m.error("I/O error", err)
		return err
	}

	if !uid && !hasChanges {
		return errors.New("No messages matched")
	}

	m.synchronize(uid, false)

	if errored {
		return errors.New("Server-side occured, only some messages affected")
	}
	return nil
}

func (m *Mailbox) CopyMessages(uid bool, seqsetRaw *imap.SeqSet, dest string) error {
	u, err := m.b.GetUser(m.username)
	if err != nil {
		m.error("", err)
		return err
	}
	_, tgtMboxI, err := u.GetMailbox(dest, true, nil)
	if err != nil {
		return err
	}
	tgtMbox := tgtMboxI.(*Mailbox)

	wrtTx, err := tgtMbox.handle.Bolt.Begin(true)
	if err != nil {
		return errors.New("I/O error, try again later")
	}
	txTgt := tgtMbox.handle.WithTransaction(wrtTx)
	defer txTgt.Rollback()

	srcTx, err := m.handle.Bolt.Begin(true)
	if err != nil {
		return errors.New("I/O error, try again later")
	}
	txSrc := m.handle.WithTransaction(srcTx)
	defer txSrc.Rollback()

	seqset, err := m.resolveSeqSet(uid, *seqsetRaw)
	if err != nil {
		return err
	}

	// Files that should be removed on transaction error.
	var purgeList []string
	rollbackFiles := func() {
		for _, f := range purgeList {
			if err := os.Remove(f); err != nil {
				m.error("purgeList %s", err, tgtMbox.Name)
			}
		}
	}

	var newTgtUids []uint32

	// TODO: Avoid iterating all messages for UID queries.
	q := txSrc.Select().OrderBy("UID")

	err = q.Each(new(message), func(rec interface{}) error {
		msg := rec.(*message)
		// srcName already includes directory.
		srcName, err := m.dir.Filename(msg.key())
		if err != nil {
			if _, ok := err.(*maildir.KeyError); ok || os.IsNotExist(err) {
				m.error("CopyMessages %s: BUG: message meta-data exists but file does not", err, msg.UID)
				txTgt.DeleteStruct(&msg)
				txTgt.DeleteStruct(&messageInfo{UID: msg.UID})
				txTgt.DeleteStruct(&messageCache{UID: msg.UID})
				// Silently skip the message as if it was not here.
				return nil
			}
			return fmt.Errorf("CopyMessages %d (src UID): filename error: %w", msg.UID, err)
		}

		if !seqset.Contains(msg.UID) {
			return nil
		}

		var info messageInfo
		if err := txSrc.One("UID", msg.UID, &info); err != nil {
			return fmt.Errorf("CopyMessages %d (src UID): info load: %w", msg.UID, err)
		}
		var cache messageCache
		if err := txSrc.One("UID", msg.UID, &cache); err != nil {
			return fmt.Errorf("CopyMessages %d (src UID): cache load: %w", msg.UID, err)
		}
		var flags messageFlags
		if err := txSrc.One("UID", msg.UID, &flags); err != nil {
			return fmt.Errorf("CopyMessages %d (src UID): flags load: %w", msg.UID, err)
		}

		m.b.Debug.Printf("CopyMessages: copying %d from %s", info.UID, m.name)

		msg.UID = 0

		if err := txTgt.Save(msg); err != nil {
			return fmt.Errorf("CopyMessages %s, %d (src UID): initial save: %w", tgtMbox.name, info.UID, err)
		}

		m.b.Debug.Printf("CopyMessages: ... as %d to %s", msg.UID, tgtMbox.name)

		tgtName := filepath.Join(tgtMbox.path, "cur", msg.key()+":2,")

		info.UID = msg.UID
		if err := txTgt.Save(&info); err != nil {
			return fmt.Errorf("CopyMessages %s, %d (tgt UID): info save: %w", tgtMbox.name, msg.UID, err)
		}

		cache.UID = msg.UID
		if err := txTgt.Save(&cache); err != nil {
			return fmt.Errorf("CopyMessages %s, %d (tgt UID): cache save: %w", tgtMbox.name, msg.UID, err)
		}

		flags.UID = msg.UID
		if err := txTgt.Save(&flags); err != nil {
			return fmt.Errorf("CopyMessages %s, %d (tgt UID): flags save: %w", tgtMbox.name, msg.UID, err)
		}

		if err := os.Link(srcName, tgtName); err != nil {
			return fmt.Errorf("CopyMessages %s, %d (tgt UID): %w", tgtMbox.name, msg.UID, err)
		}
		purgeList = append(purgeList, tgtName)

		newTgtUids = append(newTgtUids, msg.UID)

		return nil
	})
	if err != nil {
		for _, f := range purgeList {
			if err := os.Remove(f); err != nil {
				m.error("purgeList %s", err, tgtMbox.Name)
			}
		}
		if err == storm.ErrNotFound {
			if uid {
				return nil
			}
			return errors.New("No messages")
		}
		m.b.Log.Println(err)
		return errors.New("I/O error, try again later")
	}

	var mboxInfo mboxData
	if err := txTgt.One("Dummy", 1, &mboxInfo); err != nil {
		m.error("CopyMesages: target info load", err)
		return errors.New("I/O error, try again later")
	}
	mboxInfo.MsgsCount += uint32(len(purgeList))
	if err := txTgt.Save(&mboxInfo); err != nil {
		m.error("CopyMesages: target info save", err)
		return errors.New("I/O error, try again later")
	}

	for range tgtMbox.sharedHandle.conns {
		for _, uid := range newTgtUids {
			tgtMbox.sharedHandle.created <- uid
		}
	}

	if err := txTgt.Commit(); err != nil {
		m.error("CopyMessages tgt: commit", err)
		rollbackFiles()
		return errors.New("I/O error, try again later")
	}
	if err := txSrc.Commit(); err != nil {
		m.error("CopyMessages: commit src", err)
		rollbackFiles()
		return errors.New("I/O error, try again later")
	}

	if len(newTgtUids) == 0 && !uid {
		return errors.New("No messages matched")
	}

	m.synchronize(uid, false)

	return nil
}

func (m *Mailbox) Expunge() error {
	errored := false

	err := m.handle.Select().OrderBy("UID").Each(new(message), func(rec interface{}) error {
		msg := rec.(*message)
		if !msg.Deleted {
			return nil
		}

		seq := sort.Search(len(m.uidMap), func(i int) bool {
			return m.uidMap[i] >= msg.UID
		})
		if seq >= len(m.uidMap) || m.uidMap[seq] != msg.UID {
			// We should remove only messages that are known to this connection.
			return nil
		}

		if err := m.dir.Remove(msg.key()); err != nil {
			errored = true
			m.error("I/O error", err)
			return nil
		}
		if err := m.handle.DeleteStruct(&msg); err != nil {
			errored = true
			m.error("I/O error", err)
			return nil
		}

		if err := m.handle.Delete("messageInfo", msg.UID); err != nil {
			errored = true
			m.error("I/O error", err)
			return nil
		}
		if err := m.handle.Delete("messageCache", msg.UID); err != nil {
			errored = true
			m.error("I/O error", err)
			return nil
		}

		for range m.sharedHandle.conns {
			m.sharedHandle.expunged <- msg.UID
		}

		return nil
	})
	if err != nil {
		m.error("I/O error", err)
		return errors.New("I/O error")
	}

	m.synchronize(true, false)

	if errored {
		return errors.New("I/O error occured during expunge operation, not all messages are removed")
	}
	return nil
}

func (m *Mailbox) Close() error {
	if m.handle == nil {
		return nil
	}

	m.b.dbsLock.Lock()
	defer m.b.dbsLock.Unlock()

	key := m.username + "\x00" + m.name

	handle := m.b.dbs[key]
	handle.uses--

	m.b.Debug.Printf("mailbox %s/%s: session ended", m.username, m.name)

	// XXX: Race with iteration for updates.
	if m.thisConn != nil {
		foundIndx := -1
		for i, c := range handle.conns {
			if c == m.thisConn {
				foundIndx = i
			}
		}
		if foundIndx != -1 {
			copy(handle.conns[foundIndx:], handle.conns[foundIndx+1:])
			handle.conns[len(handle.conns)-1] = nil
			handle.conns = handle.conns[:len(handle.conns)-1]
			m.b.Debug.Printf("mailbox %s/%s: unregistered handle", m.username, m.name)
		} else {
			m.b.Debug.Printf("mailbox %s/%s: BUG: handle for connection was not registered", m.username, m.name)
		}
	}

	// Some sanity checks.
	if handle.uses < 0 {
		m.error("mailbox %s/%s: BUG: BoltDB reference counter went negative for", nil)
	}
	if handle.db != m.handle {
		m.error("mailbox %s/%s: BUG: multiple database handles created", nil, m.username, m.name)
	}

	if handle.uses <= 0 {
		m.b.Debug.Printf("mailbox %s/%s: freeing handle", m.username, m.name)
		delete(m.b.dbs, key)
		if err := m.handle.Close(); err != nil {
			m.error("close failed: %v", err)
			return err
		}
		return nil
	}

	m.b.dbs[key] = handle
	return nil
}

var uselessSeq = imap.Seq{
	Start: math.MaxUint32,
	Stop:  math.MaxUint32,
}

func uidToSeq(uidMap []uint32, seq imap.Seq) (imap.Seq, error) {
	if len(uidMap) == 0 {
		return uselessSeq, errors.New("No messages matched")
	}

	initial := seq

	if seq.Start == 0 {
		seq.Start = uint32(len(uidMap))
	} else if seq.Start > uidMap[len(uidMap)-1] {
		return uselessSeq, errors.New("No messages matched")
	} else if seq.Start < uidMap[0] {
		seq.Start = 1
	} else {
		seq.Start = uint32(sort.Search(len(uidMap), func(i int) bool {
			return uidMap[i] >= seq.Start
		})) + 1
	}

	if seq.Start == math.MaxUint32 {
		return uselessSeq, errors.New("No messages matched")
	}

	if seq.Stop == 0 || seq.Stop > uidMap[len(uidMap)-1] {
		seq.Stop = uint32(len(uidMap))
	} else if seq.Stop < uidMap[0] {
		return uselessSeq, errors.New("No messages matched")
	} else {
		if initial.Start == initial.Stop {
			return imap.Seq{Start: seq.Start, Stop: seq.Start}, nil
		}

		seq.Stop = uint32(sort.Search(len(uidMap), func(i int) bool {
			return uidMap[i] >= seq.Stop
		})) + 1
		if seq.Stop > uint32(len(uidMap)) || uidMap[seq.Stop-1] != initial.Stop {
			seq.Stop -= 1
		}
	}

	if seq.Start > seq.Stop || seq.Stop == math.MaxUint32 {
		return uselessSeq, errors.New("No messages matched")
	}

	return seq, nil
}

func seqToUid(uidMap []uint32, seq imap.Seq) (imap.Seq, error) {
	if len(uidMap) == 0 {
		return uselessSeq, errors.New("No messages matched")
	}

	initial := seq
	start, stop := seq.Start, seq.Stop

	for {
		if start == 0 {
			seq.Start = uidMap[len(uidMap)-1]
		} else if start > uint32(len(uidMap)) {
			return uselessSeq, errors.New("No messages matched")
		} else {
			seq.Start = uidMap[start-1]
		}

		if seq.Start != 0 {
			break
		}
		start++

		if initial.Start == initial.Stop {
			return uselessSeq, errors.New("No messages matched")
		}
	}

	if initial.Start == initial.Stop {
		return imap.Seq{Start: seq.Start, Stop: seq.Start}, nil
	}

	for {
		if stop == 0 || stop > uint32(len(uidMap)) {
			seq.Stop = uidMap[len(uidMap)-1]
		} else {
			seq.Stop = uidMap[stop-1]
		}

		if seq.Stop != 0 {
			break
		}
		stop--
		if stop == 0 {
			return uselessSeq, errors.New("No messages matched")
		}
	}

	return seq, nil
}

func (m *Mailbox) resolveSeqSet(uid bool, set imap.SeqSet) (imap.SeqSet, error) {
	if uid {
		return set, nil
	}

	result := imap.SeqSet{}
	for _, seq := range set.Set {
		seq, err := seqToUid(m.uidMap, seq)
		if err != nil {
			return imap.SeqSet{}, err
		}
		result.AddRange(seq.Start, seq.Stop)
	}
	return result, nil
}

func (m *Mailbox) uidAsSeq(uid uint32) (uint32, bool) {
	seq, err := uidToSeq(m.uidMap, imap.Seq{uid, uid})
	return seq.Start, err == nil
}

func (m *Mailbox) backgroundUpdates() {
	for {
		select {
		case <-m.stopUpdateHandling:
			return
		case msgID := <-m.sharedHandle.expunged:
			seq, ok := m.uidAsSeq(msgID)
			if !ok {
				m.error("backgroundUpdates: failed to translate msgID to sequence (msg ID = %v)", nil, msgID)
				continue
			}
			m.b.Debug.Printf("backgroundUpdates: updated sequence map for mbox instance %p", m)
			m.uidMap[seq] = 0
		case msgID := <-m.sharedHandle.created:
			m.b.Debug.Printf("backgroundUpdates: received new message ID: %v", msgID)
			m.pendingUIDs = append(m.pendingUIDs, msgID)
		case update := <-m.sharedHandle.flags:
			if update.silentFor == m {
				continue
			}
			m.b.Debug.Printf("backgroundUpdates: received updated flags for msgID %v: %v", update.uid, update.newFlags)
			m.pendingFlags = append(m.pendingFlags, update)
		}
	}
}
