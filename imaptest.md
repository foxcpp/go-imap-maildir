## imaptest status

```
35 test groups: 4 failed, 0 skipped due to missing capabilities
base protocol: 9/366 individual commands failed
extensions: 0/0 individual commands failed
```

## Scripted tests failures

### fetch-envelope

```
*** Test fetch-envelope command 1/2 (line 3)
 - failed: Missing 2 untagged replies (2 mismatches)
 - first unexpanded: 4 FETCH ($!unordered=2 ENVELOPE ("Thu, 15 Feb 2007 01:02:03 +0200" NIL (("Real Name" NIL "user" "domain")) (("Real Name" NIL "user" "domain")) (("Real Name" NIL "user" "domain")) ((NIL NIL "group" NIL) (NIL NIL "g1" "d1.org") (NIL NIL "g2" "d2.org") (NIL NIL NIL NIL) (NIL NIL "group2" NIL) (NIL NIL "g3" "d3.org") (NIL NIL NIL NIL)) ((NIL NIL "group" NIL) (NIL NIL NIL NIL) (NIL NIL "group2" NIL) (NIL NIL NIL NIL)) NIL NIL NIL))
 - first expanded: 4 FETCH ( ENVELOPE ("Thu, 15 Feb 2007 01:02:03 +0200" NIL (("Real Name" NIL "user" "domain")) (("Real Name" NIL "user" "domain")) (("Real Name" NIL "user" "domain")) ((NIL NIL "group" NIL) (NIL NIL "g1" "d1.org") (NIL NIL "g2" "d2.org") (NIL NIL NIL NIL) (NIL NIL "group2" NIL) (NIL NIL "g3" "d3.org") (NIL NIL NIL NIL)) ((NIL NIL "group" NIL) (NIL NIL NIL NIL) (NIL NIL "group2" NIL) (NIL NIL NIL NIL)) NIL NIL NIL))
 - best match: 4 FETCH (ENVELOPE ("Thu, 15 Feb 2007 01:02:03 +0200" NIL (("Real Name" NIL "user" "domain")) (("Real Name" NIL "user" "domain")) (("Real Name" NIL "user" "domain")) ((NIL NIL "g1" "d1.org") (NIL NIL "g2" "d2.org") (NIL NIL "g3" "d3.org")) NIL NIL NIL NIL))
 - Command: fetch 1:* envelope
``````

Underlying IMF parsing library (go-message) and go-imap do not support RFC 2822
group syntax. 

### search-addresses

```
*** Test search-addresses command 1/29 (line 3)
 - failed: Missing 1 untagged replies (1 mismatches)
 - first unexpanded: search 1 2 3 4 6 7
 - first expanded: search 1 2 3 4 6 7
 - best match: SEARCH 1 2 4 6 7
 - Command: search from user-from@domain.org 
```

Related From field:
```
From: <user-from (comment)@ (comment) domain.org> 
```

IMF parsing library does not normalize From addresses with comments, neither
go-imap does so.

### search-sizes

```
*** Test search-size command 2/8 (line 9)
 - failed: Missing 1 untagged replies (1 mismatches)
 - first unexpanded: search 1 2
 - first expanded: search 1 2
 - best match: SEARCH 1 2 3 4 5 6
 - Command: search smaller $size

*** Test search-size command 3/8 (line 11)
 - failed: Missing 1 untagged replies (1 mismatches)
 - first unexpanded: search 4
 - first expanded: search 4
 - best match: SEARCH
 - Command: search larger $size

*** Test search-size command 4/8 (line 13)
 - failed: Missing 1 untagged replies (1 mismatches)
 - first unexpanded: search 3 4
 - first expanded: search 3 4
 - best match: SEARCH
 - Command: search not smaller $size

*** Test search-size command 5/8 (line 15)
 - failed: Missing 1 untagged replies (1 mismatches)
 - first unexpanded: search 1 2 3
 - first expanded: search 1 2 3
 - best match: SEARCH 1 2 3 4 5 6
 - Command: search not larger $size

*** Test search-size command 6/8 (line 18)
 - failed: Missing 1 untagged replies (1 mismatches)
 - first unexpanded: search 3
 - first expanded: search 3
 - best match: SEARCH
 - Command: search not smaller $size not larger $size

*** Test search-size command 7/8 (line 20)
 - failed: Missing 1 untagged replies (1 mismatches)
 - first unexpanded: search 1 2 4
 - first expanded: search 1 2 4
 - best match: SEARCH 1 2 3 4 5 6
 - Command: search or smaller $size larger $size 
```

backendutil search function does not take header size into account.

## State tracking issues

```
Error: foxcpp[6]: seq too high (2 > 1, state=APPEND): * 2 RECENT
```
Detected randomly while running "append" scripted test. 
Investigation needed.

```
Error: foxcpp[6]: Keyword used without being in FLAGS: $keyword1: * 5 FETCH (FLAGS (\Flagged $keyword1 $keyword2 \Recent) UID 5)
Error: foxcpp[6]: Keyword used without being in FLAGS: $keyword2: * 5 FETCH (FLAGS (\Flagged $keyword1 $keyword2 \Recent) UID 5)
Error: foxcpp[7]: Keyword used without being in FLAGS: $keyword1: * 3 FETCH (FLAGS ($keyword1 $keyword2 \Recent) UID 3)
Error: foxcpp[7]: Keyword used without being in FLAGS: $keyword2: * 3 FETCH (FLAGS ($keyword1 $keyword2 \Recent) UID 3)
```
Detected while running tests involving flags. Careful RFC 3501 reading does not
seem to indicate what should be done. Likely it is imaptest being too Dovecot
specific.