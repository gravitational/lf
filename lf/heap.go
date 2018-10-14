package lf

import (
	"container/heap"
	"time"
)

// MinHeap implements heap.Interface and holds Items.
type MinHeap []*Item

func NewMinHeap() *MinHeap {
	mh := &MinHeap{}
	heap.Init(mh)
	return mh
}

func (mh MinHeap) Len() int { return len(mh) }

func (mh MinHeap) Less(i, j int) bool {
	return mh[i].Expires.Unix() < mh[j].Expires.Unix()
}

func (mh MinHeap) Swap(i, j int) {
	mh[i], mh[j] = mh[j], mh[i]
	mh[i].index = i
	mh[j].index = j
}

func (mh *MinHeap) Push(x interface{}) {
	n := len(*mh)
	item := x.(*Item)
	item.index = n
	*mh = append(*mh, item)
}

func (mh *MinHeap) Pop() interface{} {
	old := *mh
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*mh = old[0 : n-1]
	return item
}

func (mh *MinHeap) PushEl(el *Item) {
	heap.Push(mh, el)
}

func (mh *MinHeap) PopEl() *Item {
	el := heap.Pop(mh)
	return el.(*Item)
}

func (mh *MinHeap) PeekEl() *Item {
	items := *mh
	return items[0]
}

// update modifies the priority and value of an Item in the queue.
func (mh *MinHeap) UpdateEl(el *Item, expires time.Time) {
	heap.Remove(mh, el.index)
	el.Expires = expires
	heap.Push(mh, el)
}

func (mh *MinHeap) RemoveEl(el *Item) {
	heap.Remove(mh, el.index)
}
