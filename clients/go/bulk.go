package kosha

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"slices"
	"sync"
)

// BulkOptions tunes BulkIndex. Zero fields take the defaults.
type BulkOptions struct {
	// MaxDocsPerBatch closes a batch at this many documents (default 500).
	MaxDocsPerBatch int
	// MaxBytesPerBatch closes a batch at this many bytes of serialized
	// documents (default 8 MiB). A single larger document is sent alone.
	MaxBytesPerBatch int
	// Concurrency is the number of batches in flight (default 2).
	Concurrency int
	// ContinueOnError reports failed batches in the result and keeps going.
	// By default the first failure stops dispatching, in-flight batches
	// finish, and BulkIndex returns a *BulkError.
	ContinueOnError bool
}

// BulkFailure is one failed batch.
type BulkFailure struct {
	BatchIndex int
	IDs        []string
	Err        error
}

// BulkResult is what BulkIndex did.
type BulkResult struct {
	Indexed  int64
	Batches  int
	Failures []BulkFailure
}

// BulkError is a BulkIndex that stopped on a failed batch. Result holds what
// was done; Unwrap returns the first batch error.
type BulkError struct{ Result BulkResult }

func (e *BulkError) Error() string {
	f := e.Result.Failures[0]
	return fmt.Sprintf("kosha: bulk index stopped: batch %d failed: %v", f.BatchIndex, f.Err)
}

func (e *BulkError) Unwrap() error { return e.Result.Failures[0].Err }

// BulkIndex upserts docs in batches; see BulkIndexSeq.
func (n *Namespace) BulkIndex(ctx context.Context, docs []Document, opts BulkOptions, callOpts ...CallOption) (*BulkResult, error) {
	return n.BulkIndexSeq(ctx, slices.Values(docs), opts, callOpts...)
}

type flight struct {
	ids  map[string]struct{}
	done chan struct{}
}

type pending struct {
	order []string
	docs  map[string]Document
	sizes map[string]int
	bytes int
}

func newPending() *pending {
	return &pending{docs: map[string]Document{}, sizes: map[string]int{}}
}

// BulkIndexSeq upserts a stream of documents in batches, each through the
// retry policy, with bounded concurrency. A batch closes at
// MaxDocsPerBatch or MaxBytesPerBatch. A document whose id is in a batch
// still in flight waits for it, and within a batch the last occurrence of
// an id wins, so later occurrences always win. It never flushes.
func (n *Namespace) BulkIndexSeq(ctx context.Context, docs iter.Seq[Document], opts BulkOptions, callOpts ...CallOption) (*BulkResult, error) {
	maxDocs, maxBytes, conc := opts.MaxDocsPerBatch, opts.MaxBytesPerBatch, opts.Concurrency
	if maxDocs <= 0 {
		maxDocs = 500
	}
	if maxBytes <= 0 {
		maxBytes = 8 << 20
	}
	if conc <= 0 {
		conc = 2
	}

	var (
		mu       sync.Mutex
		res      BulkResult
		stopped  bool
		inflight = map[*flight]struct{}{}
		sem      = make(chan struct{}, conc)
		wg       sync.WaitGroup
		cur      = newPending()
	)
	isStopped := func() bool {
		mu.Lock()
		defer mu.Unlock()
		return stopped
	}
	dispatch := func() {
		batch := cur
		cur = newPending()
		if len(batch.order) == 0 {
			return
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			mu.Lock()
			stopped = true
			res.Failures = append(res.Failures, BulkFailure{BatchIndex: res.Batches, IDs: batch.order, Err: ctx.Err()})
			res.Batches++
			mu.Unlock()
			return
		}
		mu.Lock()
		if stopped {
			mu.Unlock()
			<-sem
			return
		}
		idx := res.Batches
		res.Batches++
		f := &flight{ids: make(map[string]struct{}, len(batch.order)), done: make(chan struct{})}
		for _, id := range batch.order {
			f.ids[id] = struct{}{}
		}
		inflight[f] = struct{}{}
		mu.Unlock()

		list := make([]Document, len(batch.order))
		for i, id := range batch.order {
			list[i] = batch.docs[id]
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := n.Upsert(ctx, list, callOpts...)
			mu.Lock()
			if err != nil {
				res.Failures = append(res.Failures, BulkFailure{BatchIndex: idx, IDs: batch.order, Err: err})
				if !opts.ContinueOnError {
					stopped = true
				}
			} else if r.IndexedCount != nil {
				res.Indexed += *r.IndexedCount
			} else {
				res.Indexed += int64(len(list))
			}
			delete(inflight, f)
			mu.Unlock()
			close(f.done)
			<-sem
		}()
	}
	blockers := func(id string) []*flight {
		mu.Lock()
		defer mu.Unlock()
		var out []*flight
		for f := range inflight {
			if _, ok := f.ids[id]; ok {
				out = append(out, f)
			}
		}
		return out
	}

	for doc := range docs {
		if isStopped() {
			break
		}
		raw, err := json.Marshal(doc)
		if err != nil {
			wg.Wait()
			return &res, configErrorf("encoding document %q: %v", doc.Id, err)
		}
		size := len(raw) + 1
		if old, dup := cur.sizes[doc.Id]; dup {
			cur.docs[doc.Id] = doc
			cur.sizes[doc.Id] = size
			cur.bytes += size - old
			continue
		}
		if len(cur.order) > 0 && (len(cur.order) >= maxDocs || cur.bytes+size > maxBytes) {
			dispatch()
		}
		if waits := blockers(doc.Id); len(waits) > 0 {
			dispatch()
			for _, f := range waits {
				<-f.done
			}
			if isStopped() {
				break
			}
		}
		cur.order = append(cur.order, doc.Id)
		cur.docs[doc.Id] = doc
		cur.sizes[doc.Id] = size
		cur.bytes += size
	}
	if !isStopped() {
		dispatch()
	}
	wg.Wait()

	slices.SortFunc(res.Failures, func(a, b BulkFailure) int { return a.BatchIndex - b.BatchIndex })
	if len(res.Failures) > 0 && !opts.ContinueOnError {
		return &res, &BulkError{Result: res}
	}
	return &res, nil
}
