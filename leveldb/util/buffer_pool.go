// Copyright (c) 2014, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package util

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type buffer struct {
	b    []byte
	miss int
}

// BufferPool is a 'buffer pool'.
type BufferPool struct {
	pool      [6]chan []byte
	size      [5]uint32
	sizeMiss  [5]uint32
	sizeHalf  [5]uint32
	baseline  [4]int // baseline:  [...]int{baseline / 4, baseline / 2, baseline * 2, baseline * 4},
	baseline0 int    // 初始化的大小 baseline

	mu     sync.RWMutex
	closed bool
	closeC chan struct{}

	get     uint32
	put     uint32
	half    uint32
	less    uint32
	equal   uint32
	greater uint32
	miss    uint32
}

// 计算n 大小的buffer 应该放置于哪个pool 中，pool 大小一共是 6，分别放置不同大小等级的buffer
func (p *BufferPool) poolNum(n int) int {
	// 最接近baseline 大小的buffer 存放于 p.pool[0] 中
	if n <= p.baseline0 && n > p.baseline0/2 {
		return 0
	}
	for i, x := range p.baseline {
		if n <= x {
			return i + 1
		}
	}
	// 最大的baseline，p.poll(len(baseline) + 1) 里面存放容量最大的buffer 缓存
	return len(p.baseline) + 1
}

// Get returns buffer with length of n.
func (p *BufferPool) Get(n int) []byte {
	if p == nil {
		return make([]byte, n)
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return make([]byte, n)
	}

	atomic.AddUint32(&p.get, 1)

	// 根据需要的buffer 大小，计算从哪个 pool 元素中找缓存的buffer
	poolNum := p.poolNum(n)
	pool := p.pool[poolNum]

	// poolNum == 0 时候，需要的buffer 大小最接近baseline 的大小
	if poolNum == 0 {
		// Fast path.
		select {
		case b := <-pool:
			switch {
			case cap(b) > n:
				// todo: 缓存的buffer 比需要的buffer 大一倍以上，这里新初始化一个n 大小的buffer 返回，并将b 重新加入pool
				if cap(b)-n >= n {
					atomic.AddUint32(&p.half, 1)
					select {
					case pool <- b:
					default:
					}
					return make([]byte, n)
				} else {
					atomic.AddUint32(&p.less, 1)
					return b[:n]
				}
			case cap(b) == n:
				atomic.AddUint32(&p.equal, 1)
				return b[:n]
			default:
				atomic.AddUint32(&p.greater, 1)
			}
		default:
			// 计数miss
			atomic.AddUint32(&p.miss, 1)
		}
		// 返回len() == n,大小为 baseline 的buffer,这里尽量是 2 的指数值，尽量保证字节对齐
		return make([]byte, n, p.baseline0)
	} else {
		sizePtr := &p.size[poolNum-1]

		select {
		case b := <-pool:
			switch {
			case cap(b) > n:
				if cap(b)-n >= n {
					atomic.AddUint32(&p.half, 1)
					sizeHalfPtr := &p.sizeHalf[poolNum-1]
					if atomic.AddUint32(sizeHalfPtr, 1) == 20 {
						atomic.StoreUint32(sizePtr, uint32(cap(b)/2))
						atomic.StoreUint32(sizeHalfPtr, 0)
					} else {
						select {
						case pool <- b:
						default:
						}
					}
					return make([]byte, n)
				} else {
					atomic.AddUint32(&p.less, 1)
					return b[:n]
				}
			case cap(b) == n:
				atomic.AddUint32(&p.equal, 1)
				return b[:n]
			default:
				atomic.AddUint32(&p.greater, 1)
				if uint32(cap(b)) >= atomic.LoadUint32(sizePtr) {
					select {
					case pool <- b:
					default:
					}
				}
			}
		default:
			atomic.AddUint32(&p.miss, 1)
		}

		if size := atomic.LoadUint32(sizePtr); uint32(n) > size {
			if size == 0 {
				atomic.CompareAndSwapUint32(sizePtr, 0, uint32(n))
			} else {
				sizeMissPtr := &p.sizeMiss[poolNum-1]
				if atomic.AddUint32(sizeMissPtr, 1) == 20 {
					atomic.StoreUint32(sizePtr, uint32(n))
					atomic.StoreUint32(sizeMissPtr, 0)
				}
			}
			return make([]byte, n)
		} else {
			return make([]byte, n, size)
		}
	}
}

// Put adds given buffer to the pool.
func (p *BufferPool) Put(b []byte) {
	if p == nil {
		return
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return
	}

	atomic.AddUint32(&p.put, 1)

	pool := p.pool[p.poolNum(cap(b))]
	select {
	case pool <- b:
	default:
	}

}

func (p *BufferPool) Close() {
	if p == nil {
		return
	}

	p.mu.Lock()
	if !p.closed {
		p.closed = true
		p.closeC <- struct{}{}
	}
	p.mu.Unlock()
}

func (p *BufferPool) String() string {
	if p == nil {
		return "<nil>"
	}

	return fmt.Sprintf("BufferPool{B·%d Z·%v Zm·%v Zh·%v G·%d P·%d H·%d <·%d =·%d >·%d M·%d}",
		p.baseline0, p.size, p.sizeMiss, p.sizeHalf, p.get, p.put, p.half, p.less, p.equal, p.greater, p.miss)
}

// 每两秒，清空buffer
func (p *BufferPool) drain() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for _, ch := range p.pool {
				select {
				case <-ch:
				default:
				}
			}
		case <-p.closeC:
			close(p.closeC)
			for _, ch := range p.pool {
				close(ch)
			}
			return
		}
	}
}

// NewBufferPool creates a new initialized 'buffer pool'.
func NewBufferPool(baseline int) *BufferPool {
	if baseline <= 0 {
		panic("baseline can't be <= 0")
	}
	p := &BufferPool{
		baseline0: baseline,
		baseline:  [...]int{baseline / 4, baseline / 2, baseline * 2, baseline * 4},
		closeC:    make(chan struct{}, 1),
	}
	// pool 中的chan 为整个Buffer 的大小，超过baseline 的buffer 只可以存在一个，和baseline 最接近的pool 可以同时有多个候选buffer
	for i, cap := range []int{2, 2, 4, 4, 2, 1} {
		p.pool[i] = make(chan []byte, cap)
	}
	go p.drain()
	return p
}
