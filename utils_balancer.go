package main

import (
	"container/heap"
)

// GranuleRequest is used to fetch all image urls from a single granule
type GranuleRequest struct {
	fn  func(url string) GranuleResult
	url string
	ch  chan GranuleResult
}

// GranuleResult holds the result of each GranuleRequest
type GranuleResult struct {
	urls []string
	err  error
}

// GranuleWorker is a worker performing GranuleRequests
type GranuleWorker struct {
	requests chan GranuleRequest // work to do (buffered channel)
	pending  int                 // count of pending tasks
	index    int                 //index in the heap
}

// Execute a request from the channel of requests
func (w *GranuleWorker) work(done chan *GranuleWorker) {
	for {
		req := <-w.requests
		req.ch <- req.fn(req.url)
		done <- w
	}
}

// Pool of available workers
type Pool []*GranuleWorker

// Return length of the heap
func (p Pool) Len() int {
	return len(p)
}

// Less checks whether the item at i-th index is smaller than item at j-th index
func (p Pool) Less(i, j int) bool {
	return p[i].pending < p[j].pending
}

// Swaps items at i-th and j-th index
func (p Pool) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
	p[i].index = i
	p[j].index = j
}

// Push an item to the heap
func (p *Pool) Push(x interface{}) {
	a := *p
	*p = append(a, x.(*GranuleWorker))
}

// Pop remove and return an item from the top of a heap
func (p *Pool) Pop() interface{} {
	old := *p
	n := len(old)
	x := old[n-1]
	*p = old[0 : n-1]

	return x
}

// Balancer balances the workload among the workers
type Balancer struct {
	pool Pool
	done chan *GranuleWorker
}

// NewBalancer creates a new balancer with a specific number of workers
func NewBalancer(nWorker int, nRequesters int) *Balancer {
	workers := make(Pool, 0, nWorker)

	done := make(chan *GranuleWorker)

	for i := 0; i < nWorker; i++ {
		w := &GranuleWorker{requests: make(chan GranuleRequest, nRequesters), pending: 0}
		go w.work(done)
		heap.Push(&workers, w)
	}

	return &Balancer{pool: workers, done: done}
}

// Balance the workload among workers
func (b Balancer) Balance(work chan GranuleRequest, abort chan error) {

	for {
		select {
		case <-abort:
			// TODO: cancel running routines, if possible
			return
		case r, ok := <-work:
			if !ok {
				return
			}
			b.dispatch(r)
		case w := <-b.done:
			b.complete(w)
		}
	}
}

// Dispatch a request to the least busy worker
func (b Balancer) dispatch(r GranuleRequest) {
	w := heap.Pop(&b.pool).(*GranuleWorker)
	w.pending++
	w.requests <- r
	heap.Push(&b.pool, w)
}

// Decrease the number of pending requests for a specific worker
func (b Balancer) complete(w *GranuleWorker) {
	w = heap.Remove(&b.pool, w.index).(*GranuleWorker)
	w.pending--
	heap.Push(&b.pool, w)
}
