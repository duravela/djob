package stop

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStopJoinsTasksThenClosesResources(t *testing.T) {
	s := NewStopper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	release := make(chan struct{})
	finished := make(chan struct{})
	if err := s.RunAsyncTask(ctx, "held", func(context.Context) {
		<-s.ShouldQuiesce()
		<-release
		close(finished)
	}); err != nil {
		t.Fatal(err)
	}
	var order []int
	for _, id := range []int{1, 2} {
		s.AddCloser(CloserFn(func() {
			select {
			case <-finished:
			default:
				t.Error("resource closed before task finished")
			}
			order = append(order, id)
		}))
	}
	stopped := make(chan struct{})
	go func() { s.Stop(ctx); close(stopped) }()
	<-s.ShouldQuiesce()
	if err := s.RunTask(ctx, "late", func(context.Context) { t.Error("late task admitted") }); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("late admission: %v", err)
	}
	select {
	case <-stopped:
		t.Fatal("cancelled context bypassed task join")
	default:
	}
	close(release)
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("stop hung")
	}
	if !slices.Equal(order, []int{2, 1}) {
		t.Fatalf("closer order: %v", order)
	}
	var late atomic.Int32
	s.AddCloser(CloserFn(func() { late.Add(1) }))
	if late.Load() != 1 {
		t.Fatal("late closer was lost")
	}
}

func TestConcurrentStopAndAdmission(t *testing.T) {
	s := NewStopper()
	var workers sync.WaitGroup
	var closed atomic.Int32
	s.AddCloser(CloserFn(func() { closed.Add(1) }))
	for range 40 {
		workers.Go(func() {
			_ = s.RunAsyncTask(context.Background(), "race", func(context.Context) { <-s.ShouldQuiesce() })
			s.Stop(context.Background())
		})
	}
	workers.Wait()
	if closed.Load() != 1 {
		t.Fatalf("closer ran %d times", closed.Load())
	}
}
