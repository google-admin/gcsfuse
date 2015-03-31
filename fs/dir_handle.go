// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fs

import (
	"fmt"
	"sort"

	"github.com/googlecloudplatform/gcsfuse/fs/inode"
	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/jacobsa/gcloud/syncutil"
	"golang.org/x/net/context"
)

// State required for reading from directories.
type dirHandle struct {
	/////////////////////////
	// Constant data
	/////////////////////////

	in *inode.DirInode

	/////////////////////////
	// Mutable state
	/////////////////////////

	Mu syncutil.InvariantMutex

	// All entries in the directory. Populated the first time we need one.
	//
	// INVARIANT: For each i, entries[i+1].Offset == entries[i].Offset + 1
	//
	// GUARDED_BY(Mu)
	entries []fuseutil.Dirent

	// Has entries yet been populated?
	//
	// INVARIANT: If !entriesValid, then len(entries) == 0
	//
	// GUARDED_BY(Mu)
	entriesValid bool
}

// Create a directory handle that obtains listings from the supplied inode.
func newDirHandle(in *inode.DirInode) (dh *dirHandle) {
	// Set up the basic struct.
	dh = &dirHandle{
		in: in,
	}

	// Set up invariant checking.
	dh.Mu = syncutil.NewInvariantMutex(dh.checkInvariants)

	return
}

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

// Dirents, sorted by name.
type sortedDirents []fuseutil.Dirent

func (p sortedDirents) Len() int           { return len(p) }
func (p sortedDirents) Less(i, j int) bool { return p[i].Name < p[j].Name }
func (p sortedDirents) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (dh *dirHandle) checkInvariants() {
	// INVARIANT: For each i, entries[i+1].Offset == entries[i].Offset + 1
	for i := 0; i < len(dh.entries)-1; i++ {
		if !(dh.entries[i+1].Offset == dh.entries[i].Offset+1) {
			panic(
				fmt.Sprintf(
					"Unexpected offset sequence: %v, %v",
					dh.entries[i].Offset,
					dh.entries[i+1].Offset))
		}
	}

	// INVARIANT: If !entriesValid, then len(entries) == 0
	if !dh.entriesValid && len(dh.entries) != 0 {
		panic("Unexpected non-empty entries slice")
	}
}

// Read some entries from the directory inode. Return newTok == "" (possibly
// with a non-empty list of entries) when the end of the directory has been
// hit.
//
// The contents of the entries' Offset fields are undefined. This function
// always behaves as if implicit directories are defined; see notes on
// DirInode.ReadEntries.
func readSomeEntries(
	ctx context.Context,
	in *inode.DirInode,
	tok string) (entries []fuseutil.Dirent, newTok string, err error) {
	entries, newTok, err = in.ReadEntries(ctx, tok)
	if err != nil {
		err = fmt.Errorf("ReadEntries: %v", err)
		return
	}

	// Return a bogus inode ID for each entry, but not the root inode ID.
	//
	// NOTE(jacobsa): As far as I can tell this is harmless. Minting and
	// returning a real inode ID is difficult because fuse does not count
	// readdir as an operation that increases the inode ID's lookup count and
	// we therefore don't get a forget for it later, but we would like to not
	// have to remember every inode ID that we've ever minted for readdir.
	//
	// If it turns out this is not harmless, we'll need to switch to something
	// like inode IDs based on (object name, generation) hashes. But then what
	// about the birthday problem? And more importantly, what about our
	// semantic of not minting a new inode ID when the generation changes due
	// to a local action?
	for i, _ := range entries {
		entries[i].Inode = fuseops.RootInodeID + 1
	}

	return
}

// Read all entries for the directory, making no effort to deal with
// conflicting names or the lack of implicit directories (see notes on
// DirInode.ReadEntries).
//
// Write entries to the supplied channel, without closing. Entry Offset fields
// have unspecified contents.
func readEntries(
	ctx context.Context,
	in *inode.DirInode,
	entries chan<- fuseutil.Dirent) (err error) {
	var tok string
	for {
		// Read a batch.
		var batch []fuseutil.Dirent

		batch, tok, err = readSomeEntries(ctx, in, tok)
		if err != nil {
			return
		}

		// Write each to the channel.
		for _, e := range batch {
			select {
			case <-ctx.Done():
				err = ctx.Err()
				return

			case entries <- e:
			}
		}

		// Are we done?
		if tok == "" {
			break
		}
	}

	return
}

// Filter out entries with type DT_Directory for which in.LookUpChild does not
// return a directory. This can be used to implicit directories without a
// matching backing object from the output of DirInode.ReadDir, which always
// behaves as if implicit directories are enabled.
func filterMissingDirectories(
	ctx context.Context,
	in *inode.DirInode,
	entriesIn <-chan fuseutil.Dirent,
	entriesOut chan<- fuseutil.Dirent) (err error) {
	err = fmt.Errorf("TODO: filterMissingDirectories")
	return
}

// Resolve name conflicts between file objects and directory objects (e.g. the
// objects "foo/bar" and "foo/bar/") by appending U+000A, which is illegal in
// GCS object names, to conflicting file names.
//
// Input must be sorted by name.
func fixConflictingNames(entries []fuseutil.Dirent) (err error) {
	// Sanity check.
	if !sort.IsSorted(sortedDirents(entries)) {
		err = fmt.Errorf("Expected sorted input")
		return
	}

	// Examine each adjacent pair of names.
	for i, _ := range entries {
		e := &entries[i]

		// Find the previous entry.
		if i == 0 {
			continue
		}

		prev := &entries[i-1]

		// Does the pair have matching names?
		if e.Name != prev.Name {
			continue
		}

		// Repair whichever is the file, remembering that there's no way to get any
		// other type in the mix.
		if e.Type == fuseutil.DT_File {
			e.Name += inode.ConflictingFileNameSuffix
		} else {
			if prev.Type != fuseutil.DT_File {
				panic(fmt.Sprintf("Unexpected type for entry: %v", prev))
			}
			prev.Name += inode.ConflictingFileNameSuffix
		}
	}

	return
}

func readAllEntries(
	ctx context.Context,
	in *inode.DirInode) (entries []fuseutil.Dirent, err error) {
	b := syncutil.NewBundle(ctx)

	// Read into a channel.
	c := make(chan fuseutil.Dirent, 100)
	b.Add(func(ctx context.Context) (err error) {
		defer close(c)
		err = readEntries(ctx, in, c)
		return
	})

	// Accumulate into the slice.
	b.Add(func(ctx context.Context) (err error) {
		for e := range c {
			entries = append(entries, e)
		}

		return
	})

	// Wait.
	err = b.Join()
	if err != nil {
		return
	}

	// Ensure that the entries are sorted, for use in fixConflictingNames
	// below.
	sort.Sort(sortedDirents(entries))

	// Fix name conflicts.
	err = fixConflictingNames(entries)
	if err != nil {
		err = fmt.Errorf("fixConflictingNames: %v", err)
		return
	}

	// Fix up offset fields.
	for i := 0; i < len(entries); i++ {
		entries[i].Offset = fuseops.DirOffset(i) + 1
	}

	return
}

////////////////////////////////////////////////////////////////////////
// Public interface
////////////////////////////////////////////////////////////////////////

// Handle a request to read from the directory, without responding.
//
// Special case: we assume that a zero offset indicates that rewinddir has been
// called (since fuse gives us no way to intercept and know for sure), and
// start the listing process over again.
//
// LOCKS_REQUIRED(dh.Mu)
func (dh *dirHandle) ReadDir(
	op *fuseops.ReadDirOp) (err error) {
	// If the request is for offset zero, we assume that either this is the first
	// call or rewinddir has been called. Reset state.
	if op.Offset == 0 {
		dh.entries = nil
		dh.entriesValid = false
	}

	// Do we need to read entries from GCS?
	if !dh.entriesValid {
		// Read entries.
		var entries []fuseutil.Dirent
		entries, err = readAllEntries(op.Context(), dh.in)
		if err != nil {
			err = fmt.Errorf("readAllEntries: %v", err)
			return
		}

		// Update state.
		dh.entries = entries
		dh.entriesValid = true
	}

	// Is the offset past the end of what we have buffered? If so, this must be
	// an invalid seekdir according to posix.
	index := int(op.Offset)
	if index > len(dh.entries) {
		err = fuse.EINVAL
		return
	}

	// We copy out entries until we run out of entries or space.
	for i := index; i < len(dh.entries); i++ {
		op.Data = fuseutil.AppendDirent(op.Data, dh.entries[i])
		if len(op.Data) > op.Size {
			op.Data = op.Data[:op.Size]
			break
		}
	}

	return
}
