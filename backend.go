package imapmaildir

import (
	"errors"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/asdine/storm/v3"
	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/backend"
)

type Backend struct {
	Log   *log.Logger
	Debug *log.Logger

	PathTemplate  string
	Authenticator func(*imap.ConnInfo, string, string) (bool, error)

	// BoltDB does not allow to open the same database file multiple times,
	// therefore we need to serialize access to one handle and close it only if
	// the mailbox is no longer used.
	//
	// dbsLock protects the concurrent map access. At the moment there is no
	// clever locking and dbsLock is held for the whole duration of storm.DB
	// initialization.  That is, once dbsLock is acquired, all elements in the
	// map have vaild db.
	//
	// Lookup key is username + \0 + mailboxName.
	dbs     map[string]*mailboxHandle
	dbsLock sync.Mutex
}

type mailboxHandle struct {
	db    *storm.DB
	uses  int64
	conns []backend.Conn

	expunged chan uint32
	created  chan uint32
	flags    chan flagUpdate
}

type flagUpdate struct {
	silentFor *Mailbox

	uid      uint32
	newFlags []string
}

func (b *Backend) Login(connInfo *imap.ConnInfo, username, password string) (backend.User, error) {
	if b.Authenticator != nil {
		ok, err := b.Authenticator(connInfo, username, password)
		if err != nil || !ok {
			if err != nil {
				b.Log.Printf("authentication error: %v", err)
			}
			return nil, backend.ErrInvalidCredentials
		}
	}

	return b.GetUser(username)
}

func (b *Backend) GetUser(username string) (backend.User, error) {
	basePath := strings.ReplaceAll(b.PathTemplate, "{username}", username)

	if _, err := os.Stat(basePath); err != nil {
		if os.IsNotExist(err) {
			return nil, backend.ErrInvalidCredentials
		}
		b.Log.Printf("%v", err)
		return nil, errors.New("I/O error")
	}

	b.Debug.Printf("user logged in (%v, %v)", username, basePath)

	return &User{
		b:        b,
		name:     username,
		basePath: basePath,
	}, nil
}

func (b *Backend) CreateUser(username string) error {
	basePath := strings.ReplaceAll(b.PathTemplate, "{username}", username)

	err := os.Mkdir(basePath, 0700)
	if err != nil {
		if os.IsExist(err) {
			return errors.New("imapmaildir: user already exits")
		}
		return err
	}
	return nil
}

func (b *Backend) SetSubscribed(_ string, _ bool) error {
	return nil /* No-op, all mailboxes are reported as subscribed */
}

func (b *Backend) Close() error {
	b.dbsLock.Lock()
	defer b.dbsLock.Unlock()

	for k, db := range b.dbs {
		if err := db.db.Close(); err != nil {
			b.Log.Printf("close failed for %s DB: %v", k, err)
		}
	}

	return nil
}

func New(pathTemplate string) (*Backend, error) {
	return &Backend{
		Log:          log.New(os.Stderr, "imapmaildir: ", 0),
		Debug:        log.New(ioutil.Discard, "imapmaildir[debug]: ", 0),
		PathTemplate: pathTemplate,
		dbs:          map[string]*mailboxHandle{},
	}, nil
}
