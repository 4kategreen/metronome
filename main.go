package main

import (
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"sync"
	"time"
)

type Result struct {
}

type DispatchedCommand struct {
	cmd string
}

type RequestedCommand struct {
	cmd string
}

func (rc *RequestedCommand) dispatch() *DispatchedCommand {
	return &DispatchedCommand {
		cmd: rc.cmd,
	}
}

type Dispatcher struct {
	in <-chan *RequestedCommand
	feedback <-chan *Result
	out chan<- *DispatchedCommand
	stop chan struct{}
}

func (d *Dispatcher) Start() {
	d.stop = make(chan struct{}, 1)
	loop: for {
		select {
		case requested := <-d.in:
			d.out <- requested.dispatch()
		case <- d.stop:
			break loop
		}
	}
}

func (d *Dispatcher) Stop() {
	close(d.stop)
}

type WorkerPool struct {
	DesiredCount int
	in <-chan *DispatchedCommand
	out chan<- *Result
	stop chan struct{}
}

func (wp *WorkerPool) Start() {
	wp.stop = make(chan struct{}, 1)

	var wg sync.WaitGroup
	for i := 0; i < wp.DesiredCount; i++ {
		wg.Add(1)
		go func() {
			loop: for {
				select {
				case command := <- wp.in:
					fmt.Printf("Working on %q\n", command.cmd)
				case <- wp.stop:
					break loop
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func (wp *WorkerPool) Stop() {
	close(wp.stop)
}

type ClockBasedTrigger struct {
	ticker time.Ticker
	cmd RequestedCommand
	out chan *RequestedCommand
	stop chan struct{}
}

func (cbt *ClockBasedTrigger) Start() {
	cbt.stop = make(chan struct{}, 1)
	loop: for {
		select {
		case <-cbt.stop:
			cbt.ticker.Stop()
			close(cbt.out)
			break loop
		case <-cbt.ticker.C:
			select {
			case cbt.out <- &cbt.cmd:
				fmt.Printf("Queued %q\n", cbt.cmd)
			default:
				fmt.Printf("Dropped %q since it is already scheduled. Consider decreasing the frequency of this task.\n", cbt.cmd)
			}
		}
	}
}

func (cbt *ClockBasedTrigger) Stop() {
	close(cbt.stop)
}

type AgendaEntry struct {
	period time.Duration
	command RequestedCommand
}

type Agenda struct {
	entries []AgendaEntry
	stop chan struct{}
}

func (a *Agenda) Start(output chan<- *RequestedCommand) {
	a.stop = make(chan struct{}, 1)
	var wg sync.WaitGroup
	var cbts []*ClockBasedTrigger
	for _, entry := range a.entries {
		wg.Add(1)

		var cbt = &ClockBasedTrigger {
			ticker: *time.NewTicker(entry.period),
			cmd: entry.command,
			out: make(chan *RequestedCommand, 1),
		}

		cbts = append(cbts, cbt)

		go func(cbt *ClockBasedTrigger) {
			cbt.Start()
			wg.Done()
		}(cbt)
	}

	// aggregate all outputs into a single output channel for the dispatcher to consume
	wg.Add(1)
	go func() {
		cases := make([]reflect.SelectCase, len(cbts))
		for i, cbt := range cbts {
			cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(cbt.out)}
		}

		remaining := len(cases)
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(a.stop)})
		for remaining > 0 {
			chosen, value, ok := reflect.Select(cases)
			if !ok {
				// Channel has been closed
				cases[chosen].Chan = reflect.ValueOf(nil)
				remaining -= 1

				if chosen == len(cases) - 1 { // Stop selected
					for _, cbt := range cbts {
						cbt.Stop()
					}
				}

				continue
			}
			output <- value.Interface().(*RequestedCommand)
		}

		close(output)
		wg.Done()
	}()

	wg.Wait()
}

func (a *Agenda) Stop() {
	close(a.stop)
}

var exampleAgenda = Agenda {
	entries: []AgendaEntry{
		AgendaEntry { period: 2 * time.Second, command: RequestedCommand { cmd: "ls" } },
		AgendaEntry { period: 3 * time.Second, command: RequestedCommand { cmd: "echo 'hello'" } },
	},
}

func main() {
	var wg sync.WaitGroup

	requestedCommands := make(chan *RequestedCommand)
	dispatchedCommands := make(chan *DispatchedCommand)

	var dispatcher = Dispatcher {in: requestedCommands, out: dispatchedCommands}
	var workerPool = WorkerPool {DesiredCount: 5, in: dispatchedCommands}

	wg.Add(1)
	go func() {
		workerPool.Start()
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		dispatcher.Start()
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		exampleAgenda.Start(requestedCommands)
		wg.Done()
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	wait: for {
		select {
		case <-done:
			break wait
		case <-interrupt:
			exampleAgenda.Stop()
			workerPool.Stop()
			dispatcher.Stop()
		}
	}
}
