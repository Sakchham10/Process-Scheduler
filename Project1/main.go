package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func main() {
	// CLI args
	f, closeFile, err := openProcessingFile(os.Args...)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile()

	// Load and parse processes
	processes, err := loadProcesses(f)
	if err != nil {
		log.Fatal(err)
	}

	// First-come, first-serve scheduling
	FCFSSchedule(os.Stdout, "First-come, first-serve", processes)
	SJFSchedule(os.Stdout, "Shortest-job-first", processes)
	SJFPrioritySchedule(os.Stdout, "Priority", processes)
	//RRSchedule(os.Stdout, "Round-robin", processes)
}

func openProcessingFile(args ...string) (*os.File, func(), error) {
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("%w: must give a scheduling file to process", ErrInvalidArgs)
	}
	// Read in CSV process CSV file
	f, err := os.Open(args[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%v: error opening scheduling file", err)
	}
	closeFn := func() {
		if err := f.Close(); err != nil {
			log.Fatalf("%v: error closing scheduling file", err)
		}
	}

	return f, closeFn, nil
}

type (
	Process struct {
		ProcessID     int64
		ArrivalTime   int64
		BurstDuration int64
		Priority      int64
		RemainingTime int64
		CompleteTime int64
		TurnAroundTime int64
		WaitTime int64
	}
	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)
type ProcessQueueArrivalOrder struct {
	processes []Process
}
func (pq *ProcessQueueArrivalOrder) AddProcess(p Process) {
	pq.processes = append(pq.processes, p)
}
func (pq *ProcessQueueArrivalOrder) RemoveProcess(index int) {
	pq.processes = append(pq.processes[:index], pq.processes[index+1:]...)
}
type ProcessQueue struct {
	processes []Process
}
func (pq *ProcessQueue) AddProcess(p Process) {
	pq.processes = append(pq.processes, p)
}
func (pq *ProcessQueue) RemoveProcess(index int) {
	pq.processes = append(pq.processes[:index], pq.processes[index+1:]...)
}
//region Schedulers

// FCFSSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func FCFSSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func SJFPrioritySchedule(w io.Writer, title string, processes []Process) { 
	var (
		totalWait       float64
		totalTurnaround float64
		schedule        = make([][]string, len(processes))
		currentTime 	int64
		pqA	ProcessQueueArrivalOrder
		pq ProcessQueue
	)
	for _,process:= range processes{
		process.RemainingTime = process.BurstDuration
		pqA.AddProcess(process)
	}
	sortArrivalQueue(pqA.processes)
	process := pqA.processes[0]
	pqA.RemoveProcess((0))
	pq.AddProcess(process)
	var count int64 = 0
	for len(pq.processes)>0{
		if process.RemainingTime == 0{
			process =  pq.processes[0]
			process.RemainingTime -=1
			
		}else{
			process.RemainingTime -=1
		}
		currentTime+=1
		if process.RemainingTime == 0{
			process.CompleteTime = currentTime
			process.TurnAroundTime = (process.CompleteTime)-(process.ArrivalTime)
			process.WaitTime = process.TurnAroundTime-process.BurstDuration
			totalWait += float64(process.WaitTime)
			totalTurnaround += float64(process.TurnAroundTime)
			schedule[count] = []string{
				fmt.Sprint(process.ProcessID),
				fmt.Sprint(process.Priority),
				fmt.Sprint(process.BurstDuration),
				fmt.Sprint(process.ArrivalTime),
				fmt.Sprint(process.WaitTime),
				fmt.Sprint(process.TurnAroundTime),
				fmt.Sprint(process.CompleteTime),
			}
			count +=1
			pq.RemoveProcess(0)
			continue
		}
		for i,p:=range pqA.processes{
			if p.ArrivalTime == currentTime{
				pq.RemoveProcess(0)
				pq.AddProcess((p))
				pq.AddProcess((process))
				pqA.RemoveProcess(i)
				sortPriorityQueue(pq.processes)
				process =  pq.processes[0]
				break
			}
		}
	}
	total := float64(len(processes))
	aveWait := float64(totalWait / total)
	aveTurnaround := float64(totalTurnaround / total)
	aveThroughput := float64(total / float64(process.CompleteTime))
	outputTitle(w, title)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)}

func SJFSchedule(w io.Writer, title string, processes []Process) { 
	var (
		totalWait       float64
		totalTurnaround float64
		schedule        = make([][]string, len(processes))
		currentTime 	int64
		pqA	ProcessQueueArrivalOrder
		pq ProcessQueue
	)
	for _,process:= range processes{
		process.RemainingTime = process.BurstDuration
		pqA.AddProcess(process)
	}
	sortArrivalQueue(pqA.processes)
	process := pqA.processes[0]
	pqA.RemoveProcess((0))
	pq.AddProcess(process)
	var count int64 = 0
	for len(pq.processes)>0{
		if process.RemainingTime == 0{
			process =  pq.processes[0]
			process.RemainingTime -=1
			
		}else{
			process.RemainingTime -=1
		}
		//increase current_time by 1
		
		currentTime+=1
		if process.RemainingTime == 0{
			process.CompleteTime = currentTime
			process.TurnAroundTime = (process.CompleteTime)-(process.ArrivalTime)
			process.WaitTime = process.TurnAroundTime-process.BurstDuration
			totalWait += float64(process.WaitTime)
			totalTurnaround += float64(process.TurnAroundTime)
			schedule[count] = []string{
				fmt.Sprint(process.ProcessID),
				fmt.Sprint(process.Priority),
				fmt.Sprint(process.BurstDuration),
				fmt.Sprint(process.ArrivalTime),
				fmt.Sprint(process.WaitTime),
				fmt.Sprint(process.TurnAroundTime),
				fmt.Sprint(process.CompleteTime),
			}
			count +=1
			pq.RemoveProcess(0)
			continue
		}
		for i,p:=range pqA.processes{
			if p.ArrivalTime == currentTime{
				pq.RemoveProcess(0)
				pq.AddProcess((p))
				pq.AddProcess((process))
				pqA.RemoveProcess(i)
				sortDeployQueue(pq.processes)
				process =  pq.processes[0]
				break
			}
		}
		
	}
	total := float64(len(processes))
	aveWait := float64(totalWait / total)
	aveTurnaround := float64(totalTurnaround / total)
	aveThroughput := float64(total / float64(process.CompleteTime))
	outputTitle(w, title)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}
func RRSchedule(w io.Writer, title string, processes []Process) {
	var (
		totalTurnaround float64
		wait       float64
		lastCompletion  float64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)

	// variables declarations
	quantum_time := int64(2)
	queue := make([]Process, 0)
	serviceTime := int64(0)

	for len(queue) > 0 || len(processes) > 0 {
		for len(processes) > 0 && processes[0].ArrivalTime <= serviceTime {
			queue = append(queue, processes[0])
			processes = processes[1:]
		}

		if len(queue) > 0 {
			p := queue[0]
			queue = queue[1:]

			// Updatig waiting time of the process
			waitingTime := serviceTime - p.ArrivalTime
			if waitingTime < 0 {
				waitingTime = 0
			}
			
			wait += float64(waitingTime)
			// Finding the duration of a particular process.
			duration := minimum(p.BurstDuration, quantum_time)

			// Update service time
			serviceTime += duration

			// Updating the completion time.
			completionTime := serviceTime
			if duration == p.BurstDuration {
				//when the process is completed.
				totalTurnaround += float64(serviceTime - p.ArrivalTime)
				lastCompletion = float64(serviceTime)
			} else {
				// when the process is not completed.
				queue = append(queue, Process{
					ProcessID:     p.ProcessID,
					ArrivalTime:   serviceTime,
					BurstDuration: p.BurstDuration - duration,
					Priority:      p.Priority,
				})
				lastCompletion = float64(serviceTime - duration)
			}

			// updating the schedule and gantt chart
			schedule[p.ProcessID-1] = []string{
				fmt.Sprint(p.ProcessID),
				fmt.Sprint(p.Priority),
				fmt.Sprint(p.BurstDuration),
				fmt.Sprint(p.ArrivalTime),
				fmt.Sprint(waitingTime),
				fmt.Sprint(serviceTime - p.ArrivalTime),
				fmt.Sprint(completionTime),
			}
			gantt = append(gantt, TimeSlice{
				PID:   p.ProcessID,
				Start: serviceTime - duration,
				Stop:  serviceTime,
			})
		} else {
			// there will be no processes in the queue.
			serviceTime = processes[0].ArrivalTime
		}
	}

	//Calculation of the averages.
	count := float64(len(schedule))
	aveTurnaround := totalTurnaround / count
	aveWait := wait / count
	aveThroughput := count / lastCompletion

	// Printing results
	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}


//endregion

//region Output helpers
func sortArrivalQueue(pq []Process){
	sort.Slice(pq,func(i,j int)bool{
		return pq[i].ArrivalTime < pq[j].ArrivalTime || (pq[i].ArrivalTime == pq[j].ArrivalTime && pq[i].BurstDuration < pq[j].BurstDuration)
	})
}

func sortDeployQueue(pq []Process){
	sort.Slice(pq,func(i,j int)bool{
		return pq[i].RemainingTime < pq[j].RemainingTime || (pq[i].RemainingTime == pq[j].RemainingTime && pq[i].ArrivalTime < pq[j].ArrivalTime)
	})
}
func sortPriorityQueue(pq []Process){
	sort.Slice(pq,func(i,j int)bool{
		return pq[i].Priority< pq[j].Priority|| (pq[i].Priority== pq[j].Priority&& pq[i].BurstDuration < pq[j].BurstDuration) || (pq[i].Priority== pq[j].Priority&& pq[i].BurstDuration == pq[j].BurstDuration && pq[i].ArrivalTime < pq[j].ArrivalTime)
	})
}

func outputTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
	_, _ = fmt.Fprintln(w, strings.Repeat(" ", len(title)/2), title)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
}

func outputGantt(w io.Writer, gantt []TimeSlice) {
	_, _ = fmt.Fprintln(w, "Gantt schedule")
	_, _ = fmt.Fprint(w, "|")
	for i := range gantt {
		pid := fmt.Sprint(gantt[i].PID)
		padding := strings.Repeat(" ", (8-len(pid))/2)
		_, _ = fmt.Fprint(w, padding, pid, padding, "|")
	}
	_, _ = fmt.Fprintln(w)
	for i := range gantt {
		_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Start), "\t")
		if len(gantt)-1 == i {
			_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Stop))
		}
	}
	_, _ = fmt.Fprintf(w, "\n\n")
}

func outputSchedule(w io.Writer, rows [][]string, wait, turnaround, throughput float64) {
	_, _ = fmt.Fprintln(w, "Schedule table")
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"ID", "Priority", "Burst", "Arrival", "Wait", "Turnaround", "Exit"})
	table.AppendBulk(rows)
	table.SetFooter([]string{"", "", "", "",
		fmt.Sprintf("Average\n%.2f", wait),
		fmt.Sprintf("Average\n%.2f", turnaround),
		fmt.Sprintf("Throughput\n%.2f/t", throughput)})
	table.Render()
}
func minimum(x, y int64) int64{
	if x < y{
		return x
	}
	return y
}

//endregion

//region Loading processes.

var ErrInvalidArgs = errors.New("invalid args")

func loadProcesses(r io.Reader) ([]Process, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: reading CSV", err)
	}

	processes := make([]Process, len(rows))
	for i := range rows {
		processes[i].ProcessID = mustStrToInt(rows[i][0])
		processes[i].BurstDuration = mustStrToInt(rows[i][1])
		processes[i].ArrivalTime = mustStrToInt(rows[i][2])
		if len(rows[i]) == 4 {
			processes[i].Priority = mustStrToInt(rows[i][3])
		}
	}

	return processes, nil
}

func mustStrToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return i
}

//endregion
