package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"reflect"
	"sync"
	"syscall"
	"time"
)

type Event struct {
	Message string
	Producer string
	Timestamp time.Time
}

var eventChannel chan Event

func init() {
	eventChannel = make(chan Event, 100)
}

func event(message string, actor string) {
	e := Event { Message: message, Producer: actor, Timestamp: time.Now() }
	select {
	case eventChannel <- e:
		break
	default:
		fmt.Fprintf(os.Stderr, "Could not publish event; %v\n", e)
	}
}

type Result struct {
	success bool
	stdout string
	stderr string
	message string
	commandId string
}

type DispatchedCommand struct {
	Cmd []string
}

func (dc *DispatchedCommand) Id() string {
	return buildId(dc.Cmd)
}

type RequestedCommand struct {
	Cmd []string
}

func (rc *RequestedCommand) dispatch() *DispatchedCommand {
	return &DispatchedCommand {
		Cmd: rc.Cmd,
	}
}

func buildId(components []string) string {
	id := ""
	for _, c := range components {
		id = id + c
	}
	return id
}

func (rc *RequestedCommand) Id() string {
	return buildId(rc.Cmd)
}

type Dispatcher struct {
	In <-chan *RequestedCommand
	Feedback <-chan *Result
	Out chan<- *DispatchedCommand
	Budget map[string]uint

	stop chan struct{}
}

func (d *Dispatcher) buildLedger() map[string] uint {
	ledger := make(map[string] uint)
	for k, _ := range d.Budget {
		ledger[k] = 0
	}
	return ledger
}

func (d *Dispatcher) Start() {
	d.stop = make(chan struct{}, 1)
	ledger := d.buildLedger()

	loop: for {
		select {
		case result := <-d.Feedback:
			ledger[result.commandId] = ledger[result.commandId] - 1
			continue loop
		case requested := <-d.In:
			id := requested.Id()
			if d.Budget[id] > ledger[id] {
				ledger[id] = ledger[id] + 1
				d.Out <- requested.dispatch()
			} else {
				event(fmt.Sprintf("Budget exceeded. Dropping %q", id), "Dispatcher")
			}
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
	In <-chan *DispatchedCommand
	Out chan<- *Result

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
				case command := <- wp.In:
					id := command.Id()
					name := command.Cmd[0]
					args := []string{}
					if len(command.Cmd) > 1 {
						args = command.Cmd[1:]
					}

					subProcess := exec.Command(name, args...)
					subProcess.SysProcAttr = &syscall.SysProcAttr { Setpgid: true }
					var stdout, stderr bytes.Buffer
					subProcess.Stdout = &stdout
					subProcess.Stderr = &stderr

					var message string
					subProcess.Start()
					if err := subProcess.Wait(); err != nil {
						if exitErr, ok := err.(*exec.ExitError); ok {
							if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
								message = fmt.Sprintf("Subprocess exited with nonzero status (%d)\n", status.ExitStatus())
							}
						} else {
							message = fmt.Sprintf("Error invoking %q: %s\v\n", name, err)
						}
						wp.Out <- &Result{success: false, stdout: stdout.String(), stderr: stderr.String(), message: message, commandId: id}
						continue loop
					}
					wp.Out <- &Result{success: true, stdout: stdout.String(), stderr: stderr.String(), commandId: id}
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
	Cmd RequestedCommand
	Out chan *RequestedCommand

	ticker time.Ticker
	stop chan struct{}
}

func (cbt *ClockBasedTrigger) Start() {
	cbt.stop = make(chan struct{}, 1)
	loop: for {
		select {
		case <-cbt.stop:
			cbt.ticker.Stop()
			close(cbt.Out)
			break loop
		case <-cbt.ticker.C:
			select {
			case cbt.Out <- &cbt.Cmd:
				event(fmt.Sprintf("Queued %q", cbt.Cmd), fmt.Sprintf("Clock Based Trigger (%s)", cbt.Cmd.Id()))
			default:
				event(fmt.Sprintf("Dropped %q since it is already scheduled. Consider decreasing the frequency of this task.", cbt.Cmd), fmt.Sprintf("Clock Based Trigger (%s)", cbt.Cmd.Id()))
			}
		}
	}
}

func (cbt *ClockBasedTrigger) Stop() {
	close(cbt.stop)
}

type AgendaEntry struct {
	Period time.Duration
	Command RequestedCommand
	ConcurrencyBudget uint
}

type Agenda struct {
	Entries []AgendaEntry
	Out chan<- *RequestedCommand

	stop chan struct{}
}

func (a *Agenda) Start() {
	a.stop = make(chan struct{}, 1)
	var wg sync.WaitGroup
	var cbts []*ClockBasedTrigger
	for _, entry := range a.Entries {
		wg.Add(1)

		var cbt = &ClockBasedTrigger {
			ticker: *time.NewTicker(entry.Period),
			Cmd: entry.Command,
			Out: make(chan *RequestedCommand, 1),
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
			cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(cbt.Out)}
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
			a.Out <- value.Interface().(*RequestedCommand)
		}

		close(a.Out)
		wg.Done()
	}()

	wg.Wait()
}

func (a *Agenda) Stop() {
	close(a.stop)
}

func (a * Agenda) ConcurrencyBudget() map[string]uint {
	budget := make(map[string]uint)
	for _, e := range a.Entries {
		budget[e.Command.Id()] = e.ConcurrencyBudget
	}
	return budget
}

type ResultsAggregator struct {
	In <-chan *Result
	Out chan<- *Result

	stop chan struct{}
}

func (ra *ResultsAggregator) Start() {
	ra.stop = make(chan struct{}, 1)
	loop: for {
		select {
		case result := <-ra.In:
			event(fmt.Sprintf("Got result: %q", result), "Results Aggregator")
			ra.Out <- result
		case <- ra.stop:
			break loop
		}
	}
}

func (ra *ResultsAggregator) Stop() {
	close(ra.stop)
}

type EventPublisher struct {
	In <-chan Event

	stop chan struct {}
}

func (ep *EventPublisher) Start() {
	ep.stop = make(chan struct{}, 1)
	loop: for {
		select {
		case e := <-ep.In:
			fmt.Printf("%v\n", e)
		case <- ep.stop:
			break loop
		}
	}

	for e := range ep.In {
		fmt.Printf("%v\n", e)
	}
}

func (ep *EventPublisher) Stop() {
	close(ep.stop)
}

var exampleAgenda = Agenda {
	Entries: []AgendaEntry{
		AgendaEntry { Period:  2 * time.Second, Command: RequestedCommand { Cmd: []string{"ls"} }, ConcurrencyBudget: 1 },
		AgendaEntry { Period:  3 * time.Second, Command: RequestedCommand { Cmd: []string{"echo", "hello"} }, ConcurrencyBudget: 5 },
		AgendaEntry { Period:  5 * time.Second, Command: RequestedCommand { Cmd: []string{"sleep", "9"} }, ConcurrencyBudget: 1 },
  	AgendaEntry { Period: 10 * time.Second, Command: RequestedCommand { Cmd: []string{"non-existent command"} }, ConcurrencyBudget: 1 },
	},
}

func main() {
	var wg sync.WaitGroup
	var eventWg sync.WaitGroup

	requestedCommands := make(chan *RequestedCommand)
	dispatchedCommands := make(chan *DispatchedCommand)
	rawResults := make(chan *Result)
	feedback := make(chan *Result, 2 * len(exampleAgenda.Entries))

	exampleAgenda.Out = requestedCommands
	var dispatcher = Dispatcher {In: requestedCommands, Out: dispatchedCommands, Feedback: feedback, Budget: exampleAgenda.ConcurrencyBudget()}
	var workerPool = WorkerPool {DesiredCount: 5, In: dispatchedCommands, Out: rawResults}
	var resultsAggregator = ResultsAggregator { In: rawResults, Out: feedback }
	var eventPublisher = EventPublisher {In: eventChannel}

	wg.Add(1)
	go func() {
		workerPool.Start()
		resultsAggregator.Stop()
		event("Worker Pool stopped", "main")
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		dispatcher.Start()
		event("Dispatcher stopped", "main")
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		exampleAgenda.Start()
		event("Agenda stopped", "main")
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		resultsAggregator.Start()
		event("Results Aggregator stopped", "main")
		wg.Done()
	}()

	eventWg.Add(1)
	go func() {
		eventPublisher.Start()
		eventWg.Done()
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(eventChannel)
		eventPublisher.Stop()
		eventWg.Wait()
		close(done)
	}()

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	wait: for {
		select {
 		case <-done:
			fmt.Printf("Graceful shutdown complete\n")
			break wait
		case <-interrupt:
			event("Starting graceful shutdown", "main")
			exampleAgenda.Stop()
			workerPool.Stop()
			dispatcher.Stop()
		}
	}
}

// TASKS
//   x Determine success/failure via exit code
//   x Build results aggregator
//   x Define concurrency limit in Agenda Entries
//   x Handle feedback and budgets in dispatcher
//   x Define "event" (or "state transition") listener. (All of these events will be forwarded to datadog)
//   - Move each actor to a separate file
//   - Move all message datatype declarations to a separate file

// Post Prototype Tasks
//   - Define an actual datatype for "ids" and ensure that they are unique
//   - Allow external configuration of agenda
//   - Sigusr to increase/decrease verbosity of events to datadog
