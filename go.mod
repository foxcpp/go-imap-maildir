module github.com/foxcpp/go-imap-maildir

go 1.13

require (
	github.com/asdine/storm v2.1.2+incompatible // indirect
	github.com/asdine/storm/v3 v3.1.0
	github.com/emersion/go-imap v1.0.4-0.20200128190657-5162c2f0c9e1
	github.com/emersion/go-maildir v0.2.0
	github.com/emersion/go-message v0.11.2
	github.com/foxcpp/go-imap-backend-tests v0.0.0-20200802090154-7e6248c85a0e
	go.etcd.io/bbolt v1.3.5
	golang.org/x/sys v0.0.0-20200223170610-d5e6a3e2c0ae // indirect
)

replace github.com/emersion/go-imap => github.com/foxcpp/go-imap v1.0.0-beta.1.0.20200802083659-cf943ff91d80
