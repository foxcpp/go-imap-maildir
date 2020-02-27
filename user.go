package imapmaildir

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/asdine/storm/v3"
	"github.com/emersion/go-imap/backend"
	"github.com/emersion/go-maildir"
	"go.etcd.io/bbolt"
)

const (
	InboxName      = "INBOX"
	HierarchySep   = "."
	MaxMboxNesting = 100

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

func (u *User) prepareMboxPath(mbox string) (fsPath string, parts []string, err error) {
	if strings.EqualFold(mbox, InboxName) {
		return u.basePath, []string{}, nil
	}

	// Verify validity before attempting to do anything.
	if len(parts) > MaxMboxNesting {
		return "", nil, errors.New("mailbox nesting limit exceeded")
	}
	fsPath = u.basePath
	nameParts := strings.Split(mbox, HierarchySep)
	for i, part := range nameParts {
		if part == "" {
			// Strip the possible trailing separator but not allow empty parts
			// in general.
			if i != len(parts)-1 {
				return "", nil, errors.New("illegal mailbox name")
			}
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

func (u *User) ListMailboxes(subscribed bool) ([]backend.Mailbox, error) {
	// TODO: Figure out a fast way to filter subscribed/unsubscribed
	// directories.

	mboxes := []backend.Mailbox{
		&Mailbox{
			// Inbox always exists.
			name: InboxName,
			path: u.basePath,
		},
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

		mboxName, err := u.mboxName(path)
		if err != nil {
			u.b.Log.Printf("error during mailboxes iteration: %v", err)
			return filepath.SkipDir
		}

		u.b.Debug.Printf("listing mbox (%v, %v)", mboxName, path)

		// Note that Mailbox object has nil handle.
		mboxes = append(mboxes, &Mailbox{
			b:        u.b,
			username: u.name,
			name:     mboxName,
			path:     path,
		})
		return nil
	})
	if err != nil {
		u.b.Log.Printf("failed to list mailboxes: %v", err)
		return nil, errors.New("I/O error")
	}

	return mboxes, nil
}

func (u *User) openDB(fsPath, mbox string) (*storm.DB, error) {
	u.b.dbsLock.Lock()
	defer u.b.dbsLock.Unlock()

	key := u.name + "\x00" + mbox
	handle, ok := u.b.dbs[key]
	if ok {
		handle.uses++
		u.b.Debug.Printf("%d uses for %s/%s mbox", handle.uses, u.name, mbox)
		u.b.dbs[key] = handle
		db := handle.db
		return db, nil
	}

	db, err := storm.Open(filepath.Join(fsPath, IndexFile))
	if err != nil {
		return nil, err
	}

	u.b.dbs[key] = mailboxHandle{
		uses: 1,
		db:   db,
	}

	return db, nil
}

func (u *User) GetMailbox(mbox string) (backend.Mailbox, error) {
	fsPath, _, err := u.prepareMboxPath(mbox)
	if err != nil {
		return nil, err
	}

	_, err = os.Stat(fsPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, backend.ErrNoSuchMailbox
		}
		u.b.Log.Printf("failed to get mailbox: %v", err)
		return nil, errors.New("I/O error")
	}

	handle, err := u.openDB(fsPath, mbox)
	if err != nil {
		u.b.Log.Printf("failed to open DB: %v", err)
		return nil, errors.New("I/O error, try again more")
	}
	err = handle.Bolt.Update(func(btx *bbolt.Tx) error {
		tx := handle.WithTransaction(btx)
		var data mboxData
		err := tx.One("Dummy", 1, &data)
		if err == nil {
			return nil
		}
		if err != storm.ErrNotFound {
			return fmt.Errorf("read mboxData: %w", err)
		}
		u.b.Debug.Printf("initializing %s/%s", u.name, mbox)

		data.Dummy = 1
		data.UidNext = 1
		data.UidValidity = uint32(time.Now().UnixNano() & 0xFFFFFFFF)
		if err := tx.Save(&data); err != nil {
			return fmt.Errorf("save mboxData: %w", err)
		}
		return nil
	})
	if err != nil {
		handle.Close()
		u.b.Log.Printf("failed to init DB: %v", err)
		return nil, errors.New("I/O error, try again later")
	}

	u.b.Debug.Printf("get mbox (%v, %v)", mbox, fsPath)
	return &Mailbox{
		b:        u.b,
		name:     mbox,
		username: u.name,
		handle:   handle,
		dir:      maildir.Dir(fsPath),
		path:     fsPath,
	}, nil
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

	u.b.Debug.Printf("delete mbox (%v, %v)", mbox, fsPath)

	return nil
}

func (u *User) RenameMailbox(existingName, newName string) error {
	if strings.EqualFold(existingName, InboxName) {
		// TODO: Handle special case of INBOX move.
		return errors.New("not implemented")
	}

	fsPathOld, _, err := u.prepareMboxPath(existingName)
	if err != nil {
		return err
	}
	fsPathNew, _, err := u.prepareMboxPath(newName)
	if err != nil {
		return err
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
