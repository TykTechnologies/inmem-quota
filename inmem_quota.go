package inmem_quota

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var bufferSize int = 100

func SetBuffer(b int) {
	bufferSize = b
}

type QuotaCtr struct {
	Total        int
	Max          int
	Allowance    int
	freshCount   int
	resetAt      time.Time
	period       int
	lastUpdateAt time.Time
}

func (ctr *QuotaCtr) Incr() {
	ctr.freshCount += 1
	ctr.lastUpdateAt = time.Now()
}

func (ctr *QuotaCtr) ReadyToDelete(age int) bool {
	if time.Now().Add(time.Second * time.Duration(age)).After(ctr.lastUpdateAt) {
		return true
	}

	return false
}

func (ctr *QuotaCtr) IsReset() bool {
	if !time.Now().After(ctr.resetAt) {
		return false
	}

	ctr.Max = ctr.Allowance
	ctr.freshCount = 0
	ctr.Total = 0
	ctr.resetAt = time.Now().Add(time.Duration(ctr.period) * time.Second)
	return true

}

func (ctr *QuotaCtr) LocalTotal() int {
	return ctr.Total + ctr.freshCount
}

func (ctr *QuotaCtr) IsBlocked() bool {
	return ctr.LocalTotal() >= ctr.Max
}

func NewQuotaCtr(period int, allowance, total int) *QuotaCtr {
	return &QuotaCtr{
		Total:        total,
		Max:          allowance,
		Allowance:    allowance,
		freshCount:   0,
		period:       period,
		resetAt:      time.Now().Add(time.Duration(period) * time.Second),
		lastUpdateAt: time.Now(),
	}
}

type Quotas struct {
	Quotas   sync.Map
	Overages sync.Map
	c        chan string
	oc       chan string
	doc      chan string
}

func NewQuotasList() *Quotas {
	q := &Quotas{
		c:   make(chan string, bufferSize),
		oc:  make(chan string, bufferSize),
		doc: make(chan string, bufferSize),
	}

	go q.Overager()
	go q.Incrementer()

	return q
}

func (q *Quotas) Incr(keyID string) error {
	select {
	case q.c <- keyID:
		return nil
	default:
		return errors.New("quota buffer full")
	}
}

func (q *Quotas) Incrementer() {
	for {
		keyID := <-q.c

		x, ok := q.Quotas.Load(keyID)
		if !ok {
			fmt.Println("can't increment non-existent quota")
			continue
		}

		v := x.(*QuotaCtr)

		if v.IsReset() {
			select {
			case q.doc <- keyID:
			default:
				fmt.Println("overage delete buffer is blocked")
			}
		}

		if v.IsBlocked() {
			select {
			case q.oc <- keyID:
				//fmt.Printf("Blocking at %v\n", v.LocalTotal())
			default:
				fmt.Println("overage buffer is blocked")
			}
		}

		v.Incr()
		q.Quotas.Store(keyID, v)
	}
}

func (q *Quotas) Overager() {
	for {
		select {
		case keyID := <-q.oc:
			_, ok := q.Overages.LoadOrStore(keyID, true)
			if ok {
				continue
			}
		case keyID := <-q.doc:
			q.Overages.Delete(keyID)
			continue
		}
	}
}

func (q *Quotas) Get(keyID string) int {
	x, ok := q.Quotas.Load(keyID)
	if !ok {
		return 0
	}
	qctr := x.(*QuotaCtr)
	return qctr.LocalTotal()
}

func (q *Quotas) IsBlocked(keyID string) bool {
	_, ok := q.Overages.Load(keyID)
	return ok
}

func (q *Quotas) NewQuota(keyID string, period int, allowance, total int) *QuotaCtr {
	ctr := NewQuotaCtr(period, allowance, total)
	q.Quotas.Store(keyID, ctr)
	return ctr
}

func (q *Quotas) Snapshot() map[string]QuotaCtr {
	m := map[string]QuotaCtr{}
	q.Quotas.Range(func(key, value interface{}) bool {
		m[key.(string)] = *value.(*QuotaCtr)
		return true
	})

	return m
}

func (q *Quotas) Merge(ext map[string]QuotaCtr) {
	for k, v := range ext {
		q.NewQuota(k, v.period, v.Allowance, v.Total)
	}
}

func (q *Quotas) Clean(maxAge int) []string {
	delete := make([]string, 0)
	q.Quotas.Range(func(key, value interface{}) bool {
		x := value.(*QuotaCtr)
		if x.ReadyToDelete(maxAge) {
			delete = append(delete, key.(string))
		}
		return true
	})

	// Make the list smaller
	for _, v := range delete {
		q.Quotas.Delete(v)
	}

	// We may need this list to remove old keys elsewhere
	return delete
}
