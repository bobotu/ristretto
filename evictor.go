package ristretto

type EvictorEvent byte

type Evictor struct {
	// policy determines what gets let in to the cache and what gets kicked out
	policy *policy
	// getBuf is a custom ring buffer implementation that gets pushed to when
	// keys are read
	getBuf *ringBuffer
	// setBuf is a buffer allowing us to batch/drop Sets during times of high
	// contention
	setBuf chan *item
	// stop is used to stop the processItems goroutine
	stop chan struct{}

	onEvict func(key uint64)
}

func NewEvictor(numItemsHint, size int64, onEvict func(key uint64)) *Evictor {
	policy := newPolicy(numItemsHint*10, size)
	e := &Evictor{
		policy:  policy,
		getBuf:  newRingBuffer(policy, 64),
		setBuf:  make(chan *item, setBufSize),
		onEvict: onEvict,
	}
	go e.processItems()
	return e
}

func (e *Evictor) Touch(key uint64) {
	e.getBuf.Push(key)
}

func (e *Evictor) Add(key uint64, cost int64) {
	e.setBuf <- &item{
		flag: itemNew,
		key:  key,
		cost: cost,
	}
}

func (e *Evictor) Del(key uint64) {
	e.setBuf <- &item{
		flag: itemDelete,
		key:  key,
	}
}

func (e *Evictor) Close() {
	e.stop <- struct{}{}
	close(e.stop)
	close(e.setBuf)
	e.policy.Close()
}

func (e *Evictor) Clear() {
	// block until processItems goroutine is returned
	e.stop <- struct{}{}
	// swap out the setBuf channel
	e.setBuf = make(chan *item, setBufSize)
	// clear value hashmap and policy data
	e.policy.Clear()
	// restart processItems goroutine
	go e.processItems()
}

// processItems is ran by goroutines processing the Set buffer.
func (e *Evictor) processItems() {
	for {
		select {
		case i := <-e.setBuf:
			switch i.flag {
			case itemNew:
				victims, added := e.policy.Add(i.key, i.cost)
				if !added {
					e.onEvict(i.key)
				}
				for _, victim := range victims {
					e.onEvict(victim.key)
				}
			case itemDelete:
				e.policy.Del(i.key) // Deals with metrics updates.
			}
		case <-e.stop:
			return
		}
	}
}
