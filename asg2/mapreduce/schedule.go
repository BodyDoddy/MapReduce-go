package mapreduce

import (
	"sync"
)

func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var numOtherPhase int
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)     // number of map tasks
		numOtherPhase = mr.nReduce // number of reducers
	case reducePhase:
		ntasks = mr.nReduce           // number of reduce tasks
		numOtherPhase = len(mr.files) // number of map tasks
	}
	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, numOtherPhase)

	tasks := make(chan int, ntasks)
	for i := 0; i < ntasks; i++ {
		tasks <- i
	}
	for len(tasks) > 0 {
		run(mr.jobName, mr.files, phase, mr.registerChannel, tasks, len(tasks), numOtherPhase)
	}
	close(tasks)
	debug("Schedule: %v phase done\n", phase)
}
func run(jobName string, files []string, phase jobPhase, registerChan chan string, tasks chan int, ntasks, n_other int) {
	Wch := make(chan int)   //workers channel
	jChan := make(chan int) // channel to receive task index
	go func() {
		for {
			select {
			case j := <-tasks:
				jChan <- j
			default:
				close(jChan)
				return
			}
		}
	}()

	for j := range jChan {
		task := RunTaskArgs{jobName, files[j], phase, j, n_other}
		go manageWorker(registerChan, task, Wch, tasks, j)
	}
	wg := &sync.WaitGroup{}

	for p := 0; p < ntasks; p++ {
		wg.Add(1)
		go func() {
			<-Wch //wait until all finish
			wg.Done()
		}()
	}

	wg.Wait()
	close(Wch)

}
func manageWorker(registerChan chan string, task RunTaskArgs, workers, tasks chan int, j int) {
	address := <-registerChan
	finished := call(address, "Worker.RunTask", task, nil)
	if finished { // current worker finished
		workers <- 1
		registerChan <- address // reuse the worker
	} else { // current worker failed
		tasks <- j
		workers <- 0
	}
}
