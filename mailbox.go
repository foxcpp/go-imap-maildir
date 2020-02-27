package imapmaildir

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/asdine/storm/v3"
	"github.com/emersion/go-imap"
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
		return nil, errors.New("I/O error")
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
			return nil, fmt.Errorf("unknown status item: %s", item)
		}
	}

	q := m.handle.Select().OrderBy("UID")
	err := q.Each(new(message), func(rec interface{}) error {
		msg := rec.(*message)
		msgCounter++
		if needUnseen && msg.Unseen {
			status.Unseen++
		}
		if status.UnseenSeqNum == 0 && msg.Unseen {
			status.UnseenSeqNum = uint32(msgCounter + 1)
		}
		return nil
	})
	if err != nil && err != storm.ErrNotFound {
		m.error("Status", err)
		return nil, errors.New("I/O error")
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

	return status, nil
}

func (m *Mailbox) SetSubscribed(subscribed bool) error {
	// No-op. Subscription status is not implemented.
	return nil
}

func (m *Mailbox) Check() error {
	// No Check necessary as changes are coordinated via single Backend instance.
	return nil
}

func (m *Mailbox) contains(seq *imap.SeqSet, id uint32, last bool) bool {
	if seq.Contains(id) {
		return true
	}

	if seq.Dynamic() {
		for _, s := range seq.Set {
			if s.Stop == 0 && last {
				return true
			}
		}
	}
	return false
}

func (m *Mailbox) ListMessages(uid bool, seqset *imap.SeqSet, items []imap.FetchItem, ch chan<- *imap.Message) error {
	defer close(ch)

	var mboxMeta mboxData
	if err := m.handle.One("Dummy", 1, &mboxMeta); err != nil {
		m.error("list: I/O error for mboxMeta", err)
		return errors.New("I/O error, try again later")
	}

	errored := false

	q := m.handle.Select().OrderBy("UID")

	maxSeqNum, err := q.Count(new(message))
	if err != nil {
		m.error("list: I/O error for mboxMeta", err)
		return errors.New("I/O error, try again later")
	}

	var seqNum uint32
	err = q.Each(new(message), func(rec interface{}) error {
		msg := rec.(*message)
		seqNum++

		if uid && !m.contains(seqset, msg.UID, seqNum == uint32(maxSeqNum)) {
			return nil
		}
		if !uid && !m.contains(seqset, seqNum, seqNum == uint32(maxSeqNum)) {
			return nil
		}

		m.b.Debug.Println("ListMessages: fetching", items, "for", seqNum, msg.UID)
		if err := m.fetch(ch, seqNum, *msg, items); err != nil {
			m.error("fetch", err)
			errored = true
			return nil
		}

		return nil
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
		// TODO: Emit StatusUpdate (or was it MailboxUpdate?).
		if err := tx.Save(&mboxMeta); err != nil {
			// TODO: Perhaps this should not fail and we can get away by having Status fix it?
			return fmt.Errorf("CreateMessage %d: save mboxData: %w", msg.UID, err)
		}

		m.b.Debug.Printf("CreateMessage: written UID %d as maildir key %s to mbox %s/%s", msg.UID, msg.key(), m.username, m.name)

		return nil
	})
	if err != nil {
		m.error("CreateMessage %d: update", err, msg.UID)
		delTemp()
		return errors.New("I/O error, try again later")
	}

	return nil
}

func (m *Mailbox) UpdateMessagesFlags(uid bool, seqset *imap.SeqSet, operation imap.FlagsOp, flags []string) error {
	var (
		mFlags  messageFlags
		errored = false
	)

	err := m.handle.Bolt.Update(func(btx *bbolt.Tx) error {
		tx := m.handle.WithTransaction(btx)

		q := tx.Select().OrderBy("UID")

		maxSeqNum, err := q.Count(new(message))
		if err != nil {
			m.error("UpdateMessagesFlags: count", err)
			return err
		}

		// TODO: Avoid iterating all messages for UID queries.
		var seqNum uint32
		return q.Each(new(message), func(rec interface{}) error {
			msg := rec.(*message)
			seqNum++

			if uid && !m.contains(seqset, msg.UID, seqNum == uint32(maxSeqNum)) {
				return nil
			}
			if !uid && !m.contains(seqset, seqNum, seqNum == uint32(maxSeqNum)) {
				return nil
			}

			if err := tx.One("UID", msg.UID, &mFlags); err != nil {
				m.error("UpdateMessagesFlags: fetch", err)
				errored = true
				return nil
			}

			m.b.Debug.Println("UpdateMessageFlags: updating flags", seqNum, msg.UID, mFlags.Flags, "op", operation, flags)
			mFlags.Flags = backendutil.UpdateFlags(mFlags.Flags, operation, flags)
			if err := tx.Save(&mFlags); err != nil {
				m.error("UpdateMessagesFlags: save", err)
				errored = true
				return nil
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

	if errored {
		return errors.New("Server-side occured, only some messages affected")
	}
	return nil
}

func (m *Mailbox) CopyMessages(uid bool, seqset *imap.SeqSet, dest string) error {
	u, err := m.b.GetUser(m.username)
	if err != nil {
		m.error("", err)
		return err
	}
	tgtMboxI, err := u.GetMailbox(dest)
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

	// Files that should be removed on transaction error.
	var purgeList []string
	rollbackFiles := func() {
		for _, f := range purgeList {
			if err := os.Remove(f); err != nil {
				m.error("purgeList %s", err, tgtMbox.Name)
			}
		}
	}

	// TODO: Avoid iterating all messages for UID queries.
	q := txSrc.Select().OrderBy("UID")

	maxSeqNum, err := q.Count(new(message))
	if err != nil {
		m.error("UpdateMessagesFlags: count", err)
		return err
	}

	var seqNum uint32
	err = q.Each(new(message), func(rec interface{}) error {
		msg := rec.(*message)
		seqNum++
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

		if uid && !m.contains(seqset, msg.UID, seqNum == uint32(maxSeqNum)) {
			return nil
		}
		if !uid && !m.contains(seqset, seqNum, seqNum == uint32(maxSeqNum)) {
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

	return nil
}

func (m *Mailbox) Expunge() error {
	var msgs []message
	if err := m.handle.AllByIndex("UID", &msgs); err != nil {
		m.error("I/O error", err)
		return errors.New("I/O error")
	}

	errored := false

	for _, msg := range msgs {
		if !msg.Deleted {
			continue
		}

		if err := m.dir.Remove(msg.key()); err != nil {
			errored = true
			m.error("I/O error", err)
			continue
		}
		if err := m.handle.DeleteStruct(&msg); err != nil {
			errored = true
			m.error("I/O error", err)
			continue
		}

		if err := m.handle.Delete("messageInfo", msg.UID); err != nil {
			errored = true
			m.error("I/O error", err)
			continue
		}
		if err := m.handle.Delete("messageCache", msg.UID); err != nil {
			errored = true
			m.error("I/O error", err)
			continue
		}
	}
	// TODO: Emit accumulated ExpungeUpdate.

	if errored {
		return errors.New("I/O error occured during expunge operation, not all messages are removed")
	}
	return nil
}

func (m *Mailbox) Close() error {
	// XXX: This function is currently not called by go-imap.
	// https://github.com/emersion/go-imap/pull/341
	// Mailbox handles __WILL__ leak.

	if m.handle == nil {
		return nil
	}

	m.b.dbsLock.Lock()
	defer m.b.dbsLock.Unlock()

	key := m.username + "\x00" + m.name

	handle := m.b.dbs[key]
	handle.uses--

	// Some sanity checks.
	if handle.uses < 0 {
		m.error("mailbox %s/%s: BUG: BoltDB reference counter went negative for", nil)
	}
	if handle.db != m.handle {
		m.error("mailbox %s/%s: BUG: multiple database handles created", nil)
	}

	if handle.uses <= 0 {
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
