package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
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

	// Shortest Job First (preemptive) scheduling
	SJFSchedule(os.Stdout, "Shortest Job First (preemptive)", processes)

	// Shortest Job First Priority (preemptive) scheduling
	SJFPrioritySchedule(os.Stdout, "Shortest Job First Priority (preemptive)", processes)

	// Round-Robin (non-preemptive) scheduling
	RRSchedule(os.Stdout, "Round-Robin (non-preemptive)", processes)
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
	}
	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)

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

func SJFSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
		remainingBurst   = make(map[int64]int64)
	)
	copyProcesses := make([]Process, len(processes))
	copy(copyProcesses, processes)

	for i := range copyProcesses {
		remainingBurst[copyProcesses[i].ProcessID] = copyProcesses[i].BurstDuration
	}

	for len(copyProcesses) > 0 {
		shortestJobIndex := 0
		for i := range copyProcesses {
			if copyProcesses[i].ArrivalTime <= serviceTime {
				if remainingBurst[copyProcesses[i].ProcessID] < remainingBurst[copyProcesses[shortestJobIndex].ProcessID] {
					shortestJobIndex = i
				}
			}
		}

		shortestJob := copyProcesses[shortestJobIndex]
		delete(remainingBurst, shortestJob.ProcessID)
		if shortestJob.ArrivalTime > serviceTime {
			waitingTime = shortestJob.ArrivalTime - serviceTime
		}
		totalWait += float64(waitingTime)

		start := serviceTime + waitingTime
		turnaround := waitingTime + shortestJob.BurstDuration
		totalTurnaround += float64(turnaround)

		completion := serviceTime + waitingTime + shortestJob.BurstDuration
		lastCompletion = float64(completion)

		schedule[len(processes)-len(copyProcesses)] = []string{
			fmt.Sprint(shortestJob.ProcessID),
			fmt.Sprint(shortestJob.Priority),
			fmt.Sprint(shortestJob.BurstDuration),
			fmt.Sprint(shortestJob.ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}

		serviceTime += turnaround

		gantt = append(gantt, TimeSlice{
			PID:   shortestJob.ProcessID,
			Start: start,
			Stop:  start + turnaround,
		})

		copyProcesses = append(copyProcesses[:shortestJobIndex], copyProcesses[shortestJobIndex+1:]...)
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

// SJFPrioritySchedule implements Shortest Job First (SJF) Priority preemptive scheduling algorithm
func SJFPrioritySchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
		remainingBurst   = make(map[int64]int64)
	)
	copyProcesses := make([]Process, len(processes))
	copy(copyProcesses, processes)

	for i := range copyProcesses {
		remainingBurst[copyProcesses[i].ProcessID] = copyProcesses[i].BurstDuration
	}

	for len(copyProcesses) > 0 {
		highestPriorityIndex := 0
		for i := range copyProcesses {
			if copyProcesses[i].ArrivalTime <= serviceTime {
				if copyProcesses[i].Priority < copyProcesses[highestPriorityIndex].Priority {
					highestPriorityIndex = i
				}
			}
		}

		highestPriorityJob := copyProcesses[highestPriorityIndex]
		delete(remainingBurst, highestPriorityJob.ProcessID)
		if highestPriorityJob.ArrivalTime > serviceTime {
			waitingTime = highestPriorityJob.ArrivalTime - serviceTime
		}
		totalWait += float64(waitingTime)

		start := serviceTime + waitingTime
		turnaround := waitingTime + highestPriorityJob.BurstDuration
		totalTurnaround += float64(turnaround)

		completion := serviceTime + waitingTime + highestPriorityJob.BurstDuration
		lastCompletion = float64(completion)

		schedule[len(processes)-len(copyProcesses)] = []string{
			fmt.Sprint(highestPriorityJob.ProcessID),
			fmt.Sprint(highestPriorityJob.Priority),
			fmt.Sprint(highestPriorityJob.BurstDuration),
			fmt.Sprint(highestPriorityJob.ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}

		serviceTime += turnaround

		gantt = append(gantt, TimeSlice{
			PID:   highestPriorityJob.ProcessID,
			Start: start,
			Stop:  start + turnaround,
		})

		copyProcesses = append(copyProcesses[:highestPriorityIndex], copyProcesses[highestPriorityIndex+1:]...)
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

// RRSchedule implements Round-Robin preemptive scheduling algorithm
func RRSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
		remainingBurst   = make(map[int64]int64)
	)
	copyProcesses := make([]Process, len(processes))
	copy(copyProcesses, processes)

	for i := range copyProcesses {
		remainingBurst[copyProcesses[i].ProcessID] = copyProcesses[i].BurstDuration
	}

	for len(copyProcesses) > 0 {
		for i := range copyProcesses {
			if copyProcesses[i].ArrivalTime <= serviceTime {
				burstTime := min(remainingBurst[copyProcesses[i].ProcessID], 1) 
				remainingBurst[copyProcesses[i].ProcessID] -= burstTime

				if remainingBurst[copyProcesses[i].ProcessID] == 0 {
					waitingTime = serviceTime + 1 - copyProcesses[i].ArrivalTime - burstTime
				} else {
					waitingTime = serviceTime - copyProcesses[i].ArrivalTime
				}
				totalWait += float64(waitingTime)

				start := serviceTime + waitingTime
				turnaround := waitingTime + burstTime
				totalTurnaround += float64(turnaround)

				completion := serviceTime + 1
				lastCompletion = float64(completion)

				schedule[len(processes)-len(copyProcesses)] = []string{
					fmt.Sprint(copyProcesses[i].ProcessID),
					fmt.Sprint(copyProcesses[i].Priority),
					fmt.Sprint(copyProcesses[i].BurstDuration),
					fmt.Sprint(copyProcesses[i].ArrivalTime),
					fmt.Sprint(waitingTime),
					fmt.Sprint(turnaround),
					fmt.Sprint(completion),
				}

				serviceTime = completion

				gantt = append(gantt, TimeSlice{
					PID:   copyProcesses[i].ProcessID,
					Start: start,
					Stop:  start + turnaround,
				})

				if remainingBurst[copyProcesses[i].ProcessID] == 0 {
					copyProcesses = append(copyProcesses[:i], copyProcesses[i+1:]...)
					break
				}
			}
		}
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

//region Output helpers

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
