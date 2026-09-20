package storage

// Size-tiered compaction: a run of at least minRun age-adjacent tables of
// similar size is merged into one. Only age-adjacent tables may be merged,
// because point reads stop at the first table holding the key: merging
// around a table in the middle would let an older version overtake it.
// Once there are more than maxTables, the cheapest adjacent window is
// merged regardless of size so the count stays bounded.
const (
	minRun    = 4
	maxTables = 10
)

func pickCompaction(tables []*table) []*table {
	var best []*table
	for i := 0; i < len(tables); {
		j := i + 1
		for j < len(tables) && similarSize(tables[i].meta.size, tables[j].meta.size) {
			j++
		}
		if j-i >= minRun && j-i >= len(best) {
			best = tables[i:j]
		}
		i = j
	}
	if best != nil || len(tables) <= maxTables {
		return best
	}
	var bestSum uint64
	for i := 0; i+minRun <= len(tables); i++ {
		var sum uint64
		for _, t := range tables[i : i+minRun] {
			sum += t.meta.size
		}
		if best == nil || sum < bestSum {
			best, bestSum = tables[i:i+minRun], sum
		}
	}
	return best
}

func similarSize(a, b uint64) bool {
	return b <= 2*a && a <= 2*b
}

func (d *db) compactLoop() {
	defer d.bg.Done()
	for {
		d.mu.Lock()
		var inputs []*table
		for !d.closed {
			if inputs = pickCompaction(d.current.tables); inputs != nil {
				break
			}
			d.bgCond.Wait()
		}
		if d.closed {
			d.mu.Unlock()
			return
		}
		d.mu.Unlock()
		if err := d.compact(inputs); err != nil {
			d.fail(err)
			return
		}
	}
}

func (d *db) compact(inputs []*table) error {
	d.installMu.Lock()
	defer d.installMu.Unlock()
	// inputs were picked before the wait for installMu. Only a Restore
	// removes tables other than this goroutine, and it removes them all.
	d.mu.RLock()
	stale := !isInput(inputs[0], d.current.tables)
	d.mu.RUnlock()
	if stale {
		return nil
	}
	return d.compactLocked(inputs)
}

// compactLocked merges inputs, which must be age-adjacent members of the
// current version. Caller holds installMu.
func (d *db) compactLocked(inputs []*table) error {
	d.mu.RLock()
	tables := d.current.tables
	d.mu.RUnlock()
	// Tombstones can go only when no older table could hold a value for
	// the same key, i.e. the output becomes the oldest table.
	bottom := inputs[len(inputs)-1] == tables[len(tables)-1]

	children := make([]internalIter, len(inputs))
	for i, t := range inputs {
		children[i] = t.iter()
	}
	out, err := d.writeTableFiltered(newMergeIter(children), bottom)
	if err != nil {
		return err
	}

	man := &manifest{lastSeq: d.flushedSeq}
	for _, t := range tables {
		if isInput(t, inputs) {
			if out != nil && t == inputs[0] {
				man.tables = append(man.tables, out.meta)
			}
			continue
		}
		man.tables = append(man.tables, t.meta)
	}
	d.mu.Lock()
	man.nextNum = d.nextNum
	man.logNum = d.oldestSegmentLocked()
	d.mu.Unlock()
	if err := writeManifest(d.dir, man); err != nil {
		return err
	}

	// Marked before install: install drops the old version's reference,
	// usually the last one, and unref unlinks only a table already marked.
	for _, t := range inputs {
		t.gone.Store(true)
	}
	d.mu.Lock()
	d.install(d.current.withCompacted(inputs, out))
	d.bgCond.Broadcast()
	d.mu.Unlock()
	if out != nil {
		out.unref()
	}
	d.compactions.Add(1)
	return nil
}

func (d *db) oldestSegmentLocked() uint64 {
	if len(d.current.imm) > 0 {
		return d.current.imm[0].seg
	}
	return d.current.mem.seg
}
