package imapmaildir

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/asdine/storm/v3"
	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/backend"
	"github.com/emersion/go-maildir"
	"go.etcd.io/bbolt"
)

const (
	InboxName      = "INBOX"
	HierarchySep   = "."
	MaxMboxNesting = 100

	MaxConnsPerMbox = 20

	IndexFile = "imapmaildir-index.db"
)

func validMboxPart(name string) bool {
	// Restrict characters that may be problematic for FS handling.

	// This is list of characters not allowed in NTFS minus 0x00 (handled
	// below), in Unix world many of these characters may be troublesome to
	// handle in shell scripts.
	if strings.ContainsAny(name, ":*?\"<>|") {
		return false
	}
	// Disallow ASCII control characters (including 0x00).
	for _, ch := range name {
		if ch < ' ' {
			return false
		}
	}
	// Prevent directory structure escaping.
	return !strings.Contains(name, "..")
}

type User struct {
	b *Backend

	name     string
	basePath string
}

func (u *User) SetSubscribed(mbox string, subscribed bool) error {
	fsPath, _, err := u.prepareMboxPath(mbox)
	if err != nil {
		return err
	}
	if subscribed {
		err = ioutil.WriteFile(filepath.Join(fsPath, "subscribed"), []byte{}, os.ModePerm)
		if err != nil {
			if os.IsNotExist(err) {
				return backend.ErrNoSuchMailbox
			}
			u.b.Log.Printf("SetSubscribed: %v", err)
			return errors.New("I/O error, try again later")
		}
	} else {
		err = os.Remove(filepath.Join(fsPath, "subscribed"))
		if err != nil && !os.IsNotExist(err) {
			u.b.Log.Printf("SetSubscribed: %v", err)
			return errors.New("I/O error, try again later")
		}
	}
	return nil
}

func (u *User) Status(name string, items []imap.StatusItem) (*imap.MailboxStatus, error) {
	_, mbox, err := u.GetMailbox(name, true, nil)
	if err != nil {
		return nil, err
	}
	defer mbox.Close()
	_, _, status, err := mbox.(*Mailbox).status(items, false, false, false)
	return status, err
}

func (u *User) prepareMboxPath(mbox string) (fsPath string, parts []string, err error) {
	if strings.EqualFold(mbox, InboxName) {
		return u.basePath, []string{}, nil
	}

	// Verify validity before attempting to do anything.
	nameParts := strings.Split(mbox, HierarchySep)
	if len(nameParts) > MaxMboxNesting {
		return "", nil, errors.New("mailbox nesting limit exceeded")
	}
	fsPath = u.basePath
	for i, part := range nameParts {
		if part == "" {
			// Strip the possible trailing separator but not allow empty parts
			// in general.
			if i != len(nameParts)-1 {
				return "", nil, errors.New("illegal mailbox name")
			}
			continue
		}
		// If mailbox path starts with INBOX. - skip that level.
		if i == 0 && strings.EqualFold(part, InboxName) {
			continue
		}

		if !validMboxPart(part) {
			u.b.Log.Printf("illegal mailbox name requested by %s: %v", u.name, mbox)
			return "", nil, errors.New("illegal mailbox name")
		}
		fsPath += string(filepath.Separator) + "." + part
	}

	return fsPath, parts, nil
}

func (u *User) mboxName(fsPath string) (string, error) {
	fsPath = strings.TrimPrefix(fsPath, u.basePath+string(filepath.Separator))
	if fsPath == "" {
		return InboxName, nil
	}

	parts := strings.Split(fsPath, string(filepath.Separator))
	if len(parts) > MaxMboxNesting {
		return "", errors.New("mailbox nesting limit exceeded")
	}

	mboxParts := make([]string, 0, len(parts))
	for _, part := range parts {
		if !strings.HasPrefix(part, ".") {
			return "", fmt.Errorf("not a maildir++ path: %v", fsPath)
		}

		mboxParts = append(mboxParts, part[1:])
	}

	return strings.Join(mboxParts, HierarchySep), nil
}

func (u *User) Username() string {
	return u.name
}

func (u *User) ListMailboxes(subscribed bool) ([]imap.MailboxInfo, error) {
	mboxes := []imap.MailboxInfo{
		{
			// Inbox always exists.
			Name:       InboxName,
			Attributes: nil,
			Delimiter:  HierarchySep,
		},
	}

	_, inboxSub := os.Stat(filepath.Join(u.basePath, "subscribed"))
	if inboxSub != nil && subscribed {
		mboxes = []imap.MailboxInfo{}
	}

	err := filepath.Walk(u.basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// Ignore errors, return as much as possible.
			u.b.Log.Printf("error during mailboxes iteration: %v", err)
			return nil
		}
		if !info.IsDir() {
			return nil
		}

		// Inbox is already added explicitly above.
		if path == u.basePath {
			return nil
		}

		if !strings.HasPrefix(info.Name(), ".") {
			return filepath.SkipDir
		}

		if subscribed {
			_, err := os.Stat(filepath.Join(path, "subscribed"))
			if err != nil {
				return nil
			}
		}

		mboxName, err := u.mboxName(path)
		if err != nil {
			u.b.Log.Printf("error during mailboxes iteration: %v", err)
			return filepath.SkipDir
		}

		u.b.Debug.Printf("listing mbox (%v, %v)", mboxName, path)

		// Note that Mailbox object has nil handle.
		mboxes = append(mboxes, imap.MailboxInfo{
			Name:       mboxName,
			Attributes: nil,
			Delimiter:  HierarchySep,
		})
		return nil
	})
	if err != nil {
		u.b.Log.Printf("failed to list mailboxes: %v", err)
		return nil, errors.New("I/O error")
	}

	return mboxes, nil
}

func (u *User) openDB(fsPath, mbox string) (*mailboxHandle, error) {
	u.b.dbsLock.Lock()
	defer u.b.dbsLock.Unlock()

	key := u.name + "\x00" + mbox
	handle, ok := u.b.dbs[key]
	if ok {
		handle.uses++
		u.b.Debug.Printf("%d uses for %s/%s mbox", handle.uses, u.name, mbox)
		u.b.dbs[key] = handle
		return handle, nil
	}

	db, err := storm.Open(filepath.Join(fsPath, IndexFile))
	if err != nil {
		return nil, err
	}

	u.b.dbs[key] = &mailboxHandle{
		uses: 1,
		db:   db,
	}

	return u.b.dbs[key], nil
}

func (u *User) GetMailbox(mbox string, readOnly bool, conn backend.Conn) (*imap.MailboxStatus, backend.Mailbox, error) {
	fsPath, _, err := u.prepareMboxPath(mbox)
	if err != nil {
		return nil, nil, err
	}

	_, err = os.Stat(fsPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, backend.ErrNoSuchMailbox
		}
		u.b.Log.Printf("failed to get mailbox: %v", err)
		return nil, nil, errors.New("I/O error")
	}

	handle, err := u.openDB(fsPath, mbox)
	if err != nil {
		u.b.Log.Printf("failed to open DB: %v", err)
		return nil, nil, errors.New("I/O error, try again more")
	}
	err = handle.db.Bolt.Update(func(btx *bbolt.Tx) error {
		tx := handle.db.WithTransaction(btx)
		var data mboxData
		err := tx.One("Dummy", 1, &data)
		if err == nil {
			return nil
		}
		if err != storm.ErrNotFound {
			return fmt.Errorf("read mboxData: %w", err)
		}

		data.Dummy = 1
		data.UidNext = 1
		data.UidValidity = uint32(time.Now().UnixNano() & 0xFFFFFFFF)
		data.UsedFlags = make(map[string]int)
		u.b.Debug.Printf("initializing %s/%s with uidvalidity %v", u.name, mbox, data.UidValidity)
		if err := tx.Save(&data); err != nil {
			return fmt.Errorf("save mboxData: %w", err)
		}
		return nil
	})
	if err != nil {
		handle.db.Close()
		u.b.Log.Printf("failed to init DB: %v", err)
		return nil, nil, errors.New("I/O error, try again later")
	}

	u.b.Debug.Printf("get mbox (%v, %v)", mbox, fsPath)
	mboxHandle := &Mailbox{
		b:            u.b,
		user:         u,
		name:         mbox,
		username:     u.name,
		handle:       handle.db,
		dir:          maildir.Dir(fsPath),
		path:         fsPath,
		thisConn:     conn,
		sharedHandle: handle,
	}

	if conn == nil {
		return nil, mboxHandle, nil
	}

	uidMap, recent, status, err := mboxHandle.status([]imap.StatusItem{imap.StatusMessages, imap.StatusRecent,
		imap.StatusUnseen, imap.StatusUidNext, imap.StatusUidValidity}, true, !readOnly, true)
	if err != nil {
		return nil, mboxHandle, err
	}

	handle.mboxesLock.Lock()
	handle.mboxes = append(handle.mboxes, mboxHandle)
	handle.mboxesLock.Unlock()

	mboxHandle.uidMap = uidMap
	mboxHandle.recentUIDs = recent
	u.b.Debug.Printf("%d recent messages in %s", len(recent), mbox)
	if len(uidMap) != 0 {
		u.b.Debug.Printf("populated mailbox %s uidMap (len = %v, first = %v, last = %v)", mbox, len(uidMap), uidMap[0], uidMap[len(uidMap)-1])
	} else {
		u.b.Debug.Printf("empty mailbox %s uidMap", mbox)
	}

	return status, mboxHandle, nil
}

func (u *User) CreateMailbox(mbox string) error {
	if strings.EqualFold(mbox, InboxName) {
		return backend.ErrMailboxAlreadyExists
	}

	fsPath, _, err := u.prepareMboxPath(mbox)
	if err != nil {
		return err
	}

	if _, err := os.Stat(fsPath); err != nil {
		if !os.IsNotExist(err) {
			u.b.Debug.Printf("failed to create mailbox: %v", err)
			return errors.New("I/O error")
		}
	} else {
		return backend.ErrMailboxAlreadyExists
	}

	if err := os.MkdirAll(fsPath, 0700); err != nil {
		u.b.Debug.Printf("failed to create mailbox: %v", err)
		return errors.New("I/O error")
	}
	if err := os.MkdirAll(filepath.Join(fsPath, "cur"), 0700); err != nil {
		u.b.Debug.Printf("failed to create mailbox: %v", err)
		return errors.New("I/O error")
	}
	if err := os.MkdirAll(filepath.Join(fsPath, "new"), 0700); err != nil {
		u.b.Debug.Printf("failed to create mailbox: %v", err)
		return errors.New("I/O error")
	}
	if err := os.MkdirAll(filepath.Join(fsPath, "tmp"), 0700); err != nil {
		u.b.Debug.Printf("failed to create mailbox: %v", err)
		return errors.New("I/O error")
	}
	// IMAP index will be created on demand on first SELECT.

	u.b.Debug.Printf("create mbox (%v, %v)", mbox, fsPath)

	return nil
}

func (u *User) DeleteMailbox(mbox string) error {
	if strings.EqualFold(mbox, InboxName) {
		return errors.New("cannot delete inbox")
	}

	fsPath, _, err := u.prepareMboxPath(mbox)
	if err != nil {
		return err
	}

	if _, err := os.Stat(fsPath); err != nil {
		if os.IsNotExist(err) {
			return backend.ErrNoSuchMailbox
		}
		u.b.Debug.Printf("failed to delete mailbox: %v", err)
		return errors.New("I/O error")
	}

	// Delete in that order to
	// 1. Prevent IMAP SELECT.
	if err := os.RemoveAll(filepath.Join(fsPath, IndexFile)); err != nil {
		if !os.IsNotExist(err) {
			u.b.Log.Printf("failed to remove mailbox: %v", err)
			return errors.New("I/O error")
		}
	}

	u.b.dbsLock.Lock()
	key := u.name + "\x00" + mbox
	sharedHandle := u.b.dbs[key]
	if sharedHandle != nil {
		sharedHandle.db.Close()
		delete(u.b.dbs, key)
	}
	u.b.dbsLock.Unlock()

	// 2. Prevent new maildir deliveries.
	if err := os.RemoveAll(filepath.Join(fsPath, "tmp")); err != nil {
		if !os.IsNotExist(err) {
			u.b.Log.Printf("failed to remove mailbox: %v", err)
			return errors.New("I/O error")
		}
	}
	// 3. Prevent in-flight maildir deliveries from completing.
	if err := os.RemoveAll(filepath.Join(fsPath, "new")); err != nil {
		if !os.IsNotExist(err) {
			u.b.Log.Printf("failed to remove mailbox: %v", err)
			return errors.New("I/O error")
		}
	}
	// ... and remove all messages
	if err := os.RemoveAll(filepath.Join(fsPath, "cur")); err != nil {
		if !os.IsNotExist(err) {
			u.b.Log.Printf("failed to remove mailbox: %v", err)
			return errors.New("I/O error")
		}
	}

	// TODO: Do we want to preserve child mailboxes and follow RFC 3501
	// exactly here?
	if err := os.RemoveAll(fsPath); err != nil {
		if !os.IsNotExist(err) {
			u.b.Log.Printf("failed to remove mailbox: %v", err)
			return errors.New("I/O error")
		}
	}

	u.b.Debug.Printf("delete mbox (%v, %v)", mbox, fsPath)

	return nil
}

func (u *User) RenameMailbox(existingName, newName string) error {
	if strings.EqualFold(existingName, InboxName) {
		fsPathNew, _, err := u.prepareMboxPath(newName)
		if err != nil {
			return err
		}

		if _, err := os.Stat(fsPathNew); err != nil {
			if !os.IsNotExist(err) {
				u.b.Debug.Printf("failed to create mailbox: %v", err)
				return errors.New("I/O error")
			}
		} else {
			return backend.ErrMailboxAlreadyExists
		}

		if err := os.MkdirAll(fsPathNew, 0700); err != nil {
			u.b.Debug.Printf("failed to create mailbox: %v", err)
			return errors.New("I/O error")
		}

		if err := os.Rename(filepath.Join(u.basePath, "cur"), filepath.Join(fsPathNew, "cur")); err != nil {
			u.b.Log.Printf("failed to rename mailbox: %v", err)
			return errors.New("I/O error")
		}
		if err := os.Rename(filepath.Join(u.basePath, "new"), filepath.Join(fsPathNew, "new")); err != nil {
			u.b.Log.Printf("failed to rename mailbox: %v", err)
			return errors.New("I/O error")
		}
		if err := os.Rename(filepath.Join(u.basePath, "tmp"), filepath.Join(fsPathNew, "tmp")); err != nil {
			u.b.Log.Printf("failed to rename mailbox: %v", err)
			return errors.New("I/O error")
		}
		// Index for INBOX might be uninitialized if it was never opened.
		_ = os.Rename(filepath.Join(u.basePath, IndexFile), filepath.Join(fsPathNew, IndexFile))

		u.b.dbsLock.Lock()
		key := u.name + "\x00" + InboxName
		sharedHandle := u.b.dbs[key]
		if sharedHandle != nil {
			sharedHandle.db.Close()
			delete(u.b.dbs, key)
		}
		u.b.dbsLock.Unlock()

		return nil
	}

	fsPathOld, _, err := u.prepareMboxPath(existingName)
	if err != nil {
		return err
	}
	fsPathNew, _, err := u.prepareMboxPath(newName)
	if err != nil {
		return err
	}

	key := u.name + "\x00" + existingName
	sharedHandle := u.b.dbs[key]
	if sharedHandle != nil {
		sharedHandle.db.Close()
		delete(u.b.dbs, key)
	}

	if err := os.Rename(fsPathOld, fsPathNew); err != nil {
		u.b.Log.Printf("failed to rename mailbox: %v", err)
		return errors.New("I/O error")
	}
	u.b.Debug.Printf("rename mbox (%v, %v), (%v, %v)", existingName, fsPathOld, newName, fsPathNew)

	return nil
}

func (u *User) Logout() error {
	u.b.Debug.Printf("user logged out (%v, %v)", u.name, u.basePath)
	return nil
}

func (u *User) CreateMessage(mboxName string, flags []string, date time.Time, body imap.Literal) error {
	_, mbox, err := u.GetMailbox(mboxName, false, nil)
	if err != nil {
		return err
	}
	defer mbox.Close()
	return mbox.(*Mailbox).CreateMessage(flags, date, body)
}
