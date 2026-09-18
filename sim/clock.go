package sim

import "time"

// clock hands out timers that fire as events; time moves only when the
// loop pops one.
type clock struct {
	s    *Sim
	node *node
}

func (c *clock) Now() time.Time {
	return time.Time{}.Add(c.s.now)
}

func (c *clock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	c.node.timer = ch
	c.s.schedule(d, &event{kind: evTick, node: c.node.id, gen: c.node.gen})
	return ch
}
