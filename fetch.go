package imapmaildir

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/asdine/storm/v3"
	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/backend/backendutil"
	"github.com/emersion/go-message/textproto"
)

func (m *Mailbox) fetch(tx storm.Node, ch chan<- *imap.Message, seqNum uint32, msg message, items []imap.FetchItem) error {
	result := imap.NewMessage(seqNum, items)

	var (
		info  messageInfo
		cache messageCache
		flags messageFlags

		bodyItems []imap.FetchItem
		header    textproto.Header
	)

	for _, item := range items {
		switch item {
		case imap.FetchUid:
			result.Uid = msg.UID
		case imap.FetchFlags:
			if flags.UID == 0 {
				if err := tx.One("UID", msg.UID, &flags); err != nil {
					return fmt.Errorf("fetch: flags query: %w", err)
				}
				m.sanityCheckFlags(&msg, flags.Flags)
			}
			m.b.Debug.Println("fetch: loaded flags for uid", msg.UID, flags.Flags)
			result.Flags = flags.Flags
		case imap.FetchInternalDate:
			if info.UID == 0 {
				if err := tx.One("UID", msg.UID, &info); err != nil {
					return fmt.Errorf("fetch: info query: %w", err)
				}
			}
			m.b.Debug.Println("fetch: loaded date for uid", msg.UID, flags.Flags)
			result.InternalDate = info.InternalDate
		case imap.FetchRFC822Size:
			if info.UID == 0 {
				if err := tx.One("UID", msg.UID, &info); err != nil {
					return fmt.Errorf("fetch: info query: %w", err)
				}
			}
			result.Size = info.RFC822Size
		case imap.FetchEnvelope:
			if cache.UID == 0 {
				if err := tx.One("UID", msg.UID, &cache); err != nil {
					return fmt.Errorf("fetch: cache query: %w", err)
				}
			}
			if cache.Envelope != nil {
				result.Envelope = cache.Envelope
				m.b.Debug.Println("fetch: cache hit for envelope", msg.UID, cache.Envelope)
				continue
			}
			bodyItems = append(bodyItems, item)
		case imap.FetchBodyStructure, imap.FetchBody:
			if cache.UID == 0 {
				if err := tx.One("UID", msg.UID, &cache); err != nil {
					return fmt.Errorf("fetch: cache query: %w", err)
				}
			}
			if cache.BodyStructure != nil {
				if item == imap.FetchBody {
					result.BodyStructure = stripExtBodyStruct(cache.BodyStructure)
				} else {
					result.BodyStructure = cache.BodyStructure
				}
				m.b.Debug.Println("fetch: cache hit for body structure", msg.UID, cache.BodyStructure)
				continue
			}
			bodyItems = append(bodyItems, item)
		default:
			bodyItems = append(bodyItems, item)
		}
	}

	filePath, err := m.dir.Filename(msg.key())
	if err != nil {
		return fmt.Errorf("fetch: filename resolve: %w", err)
	}

	for _, item := range bodyItems {
		err := m.fetchBodyItem(result, &header, filePath, item)
		if err != nil {
			return err
		}
	}

	ch <- result

	return nil
}

func (m *Mailbox) fetchBodyItem(result *imap.Message, header *textproto.Header, filePath string, item imap.FetchItem) error {
	// TODO: Figure out how to avoid re-opening the file each time.
	openBody := func() (*bufio.Reader, io.Closer, error) {
		f, err := os.Open(filePath)
		return bufio.NewReader(f), f, err
	}
	ensureHeader := func(bufR *bufio.Reader) error {
		if header.Len() != 0 {
			return skipHeader(bufR)
		}
		hdr, err := textproto.ReadHeader(bufR)
		if err != nil {
			return err
		}
		*header = hdr
		return nil
	}

	switch item {
	case imap.FetchEnvelope:
		bufR, cl, err := openBody()
		if err != nil {
			return err
		}
		defer cl.Close()
		if err := ensureHeader(bufR); err != nil {
			return err
		}

		env, err := backendutil.FetchEnvelope(*header)
		if err != nil {
			return err
		}
		result.Envelope = env
	case imap.FetchBodyStructure, imap.FetchBody:
		bufR, cl, err := openBody()
		if err != nil {
			return err
		}
		defer cl.Close()
		if err := ensureHeader(bufR); err != nil {
			return err
		}

		bs, err := backendutil.FetchBodyStructure(*header, bufR, item == imap.FetchBodyStructure)
		if err != nil {
			return err
		}
		result.BodyStructure = bs
	default:
		sectName, err := imap.ParseBodySectionName(item)
		if err != nil {
			return err
		}

		bufR, cl, err := openBody()
		if err != nil {
			return err
		}
		defer cl.Close()
		if err := ensureHeader(bufR); err != nil {
			return err
		}

		literal, err := backendutil.FetchBodySection(*header, bufR, sectName)
		if err != nil {
			m.error("body section fetch %s %v", err, filePath, sectName)
			literal = bytes.NewReader(nil)
		}
		result.Body[sectName] = literal
	}

	return nil
}

func skipHeader(bufR *bufio.Reader) error {
	for {
		// Skip header if it is not needed.
		line, err := bufR.ReadSlice('\n')
		if err != nil {
			return err
		}
		// If line is empty (message uses LF delim) or contains only CR (messages uses CRLF delim)
		if len(line) == 0 || (len(line) == 1 || line[0] == '\r') {
			break
		}
	}
	return nil
}

func stripExtBodyStruct(extended *imap.BodyStructure) *imap.BodyStructure {
	stripped := *extended
	stripped.Extended = false
	stripped.Disposition = ""
	stripped.DispositionParams = nil
	stripped.Language = nil
	stripped.Location = nil
	stripped.MD5 = ""

	for i := range stripped.Parts {
		stripped.Parts[i] = stripExtBodyStruct(stripped.Parts[i])
	}
	return &stripped
}

func (m *Mailbox) sanityCheckFlags(msg *message, flags []string) {
	var (
		hasSeen    = false
		hasDeleted = false
	)
	for _, f := range flags {
		if f == imap.DeletedFlag {
			hasDeleted = true
		}
		if f == imap.SeenFlag {
			hasSeen = true
		}
	}
	if hasSeen != !msg.Unseen {
		m.error("BUG: message-messageFlags mismatch, flags: %v, message: %+v", nil, flags, msg)
	}
	if hasDeleted != msg.Deleted {
		m.error("BUG: message-messageFlags mismatch, flags: %v, message: %+v", nil, flags, msg)
	}
}
