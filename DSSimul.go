package main

import (
	"bufio"
	"container/heap"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

//tics = units pf measure
const (
	latencyTic = time.Duration(100)*time.Millisecond
	notifierTic = time.Duration(100)*time.Millisecond
	timerTic = time.Duration(100)*time.Millisecond
	waitTic = time.Duration(100)*time.Millisecond
)

type Message struct {
	SendTime, DeliveryTime time.Time
	From, To int
	Args []interface{}
}

func NewMessage(from, to int, args ...interface{}) *Message {
	var message Message
	message.From, message.To, message.SendTime = from, to, time.Now()// time.Now().Add(time.Second)
	message.Args = append(message.Args, args...)
	return &message
}

func (Self* Message) GetValue() interface{} {
	val := Self.Args[0]
	Self.Args[0] = nil
	Self.Args = Self.Args[1:]
	return val
}
func (Self* Message) IsMyMessage(prefix string) bool {
	message := Self.Args[0].(string)
	if strings.HasPrefix(message, "*") || strings.HasPrefix(message, prefix) {
		return true
	}
	return false
}

// A PriorityQueue implements heap.Interface and holds Messages.
type PriorityQueue []*Message
func (pq PriorityQueue) Len() int { return len(pq) }
func (pq PriorityQueue) Less(i, j int) bool { return pq[i].DeliveryTime.Before(pq[j].DeliveryTime) }
func (pq PriorityQueue) Swap(i, j int) { pq[i], pq[j] = pq[j], pq[i] }
func (pq *PriorityQueue) Push(x any) { item := x.(*Message); *pq = append(*pq, item) }
func (pq *PriorityQueue) Pop() (x any) { n := pq.Len(); x = (*pq)[n-1]; *pq = (*pq)[0 : n-1]; return }

type MessageQueue struct {
	priorityQueue PriorityQueue
	mutex sync.Mutex
	cond *sync.Cond
}
func (Self *MessageQueue) Dequeue() (m *Message) {
	Self.mutex.Lock()
	defer Self.mutex.Unlock()
	for Self.priorityQueue.Len() == 0 {
		Self.cond.Wait()
	}
	m = Self.priorityQueue[Self.priorityQueue.Len()-1]
	//fmt.Println(m.DeliveryTime, time.Now())
	for m.DeliveryTime.After(time.Now()) {
		//fmt.Println(m.DeliveryTime, time.Now())
		Self.cond.Wait()
		m = Self.priorityQueue[Self.priorityQueue.Len()-1]
	}
	return heap.Pop(&Self.priorityQueue).(*Message)
}
func (Self *MessageQueue) Enqueue(message *Message) {
	Self.mutex.Lock()
	defer Self.mutex.Unlock()
	heap.Push(&Self.priorityQueue, message)
	Self.cond.Signal()
}
func (Self *MessageQueue) Peek() *Message {
	Self.mutex.Lock()
	defer Self.mutex.Unlock()
	return Self.priorityQueue[Self.priorityQueue.Len()-1]
}
func (Self *MessageQueue) Size() int {
	Self.mutex.Lock()
	defer Self.mutex.Unlock()
	return Self.priorityQueue.Len()
}

type NetworkLayer struct {
	QueueMap map[int]*MessageQueue
	networkMap map[int]map[int]int
	threads []int
}
func (Self *NetworkLayer) CreateLink(from, to, cost int, bidirectional bool) {
	if from == to { return }
	_, ok := Self.networkMap[from]
	if !ok {
		Self.networkMap[from] = make(map[int]int)
	}
	Self.networkMap[from][to] = cost
	if bidirectional {
		Self.CreateLink(to, from, cost, false)
	}
}
func (Self *NetworkLayer) AddLinksToAll(from, cost int, bidirectional bool) {
	for to := range Self.threads {
		Self.CreateLink(from, to, cost, bidirectional)
	}
}
func (Self *NetworkLayer) AddLinksFromAll(to, cost int, bidirectional bool) {
	for from := range Self.threads {
		Self.CreateLink(from, to, cost, bidirectional)
	}
}
func (Self *NetworkLayer) AddLinksAllToAll(cost int) {
	for from := range Self.threads {
		Self.AddLinksToAll(from, cost, false)
	}
}
func (Self *NetworkLayer) GetLink(from, to int) (cost int) {
	if from == to || from < 0 { return 0}
	resMap, ok := Self.networkMap[from]
	if !ok {return -1}
	cost, ok = resMap[to]
	if !ok {return -1}
	return
}
func (Self *NetworkLayer) Send(from, to int, args ...interface{}) {
	if to < 0 {
		for i, qM := range Self.QueueMap{
			message := NewMessage(from, i, args...)
			message.DeliveryTime = message.SendTime.Add(time.Duration(Self.GetLink(from, i))*latencyTic)
			qM.Enqueue(message)
		}
		return
	}
	message := NewMessage(from, to, args...)
	message.DeliveryTime = message.SendTime.Add(time.Duration(Self.GetLink(from, to))*latencyTic)
	Self.QueueMap[to].Enqueue(message)
}

func (Self *NetworkLayer) Neighbours(from int) (neighbours []int){
	Map, ok := Self.networkMap[from]
	if !ok {return }
	for key := range Map {
		neighbours = append(neighbours, key)
	}
	return
}


type workFunction func(process *Process, message Message)

type Process struct {
	networkLayer *NetworkLayer
	messageQueue *MessageQueue
	node int
	context Context
	workers []workFunction
	//waitGroup *sync.WaitGroup
}

func (Self* Process) RegisterWorkFunction(function workFunction){
	Self.workers = append(Self.workers, function)
}
func (Self* Process) Neighbours() []int {
	return Self.networkLayer.Neighbours(Self.node)
}

func (Self *Process) WorkerThreadExecutor() {
	//defer Self.waitGroup.Done()
	//fmt.Printf("%v alive!\n", Self.node)
	for {
		m := Self.messageQueue.Dequeue()
		for _, f := range Self.workers {
			 var message Message = *m
			f(Self, message)
		}
	}
}

func (Self *NetworkLayer) registerProcess(node int, p* Process) {
	p.node = node
	Self.threads = append(Self.threads, node)
	Self.QueueMap[node] = &MessageQueue{}
	Self.QueueMap[node].mutex = sync.Mutex{}
	Self.QueueMap[node].cond = sync.NewCond(&Self.QueueMap[node].mutex)
	p.networkLayer = Self
	p.messageQueue = Self.QueueMap[node]
}

type World struct {
	processMap   map[int]*Process
	networkLayer NetworkLayer
	associates map[string]workFunction
	//waitGroup sync.WaitGroup
	//timeout time.Duration
}

func NewWorld() (world World) {
	world.networkLayer.QueueMap = make(map[int]*MessageQueue)
	world.networkLayer.networkMap = make(map[int]map[int]int)
	world.associates = make(map[string]workFunction)
	world.processMap = make(map[int]*Process)
	return
}

func (world *World) createProcess(node int) {
	var p Process
	world.networkLayer.registerProcess(node, &p)
	world.processMap[node] = &p
	//world.waitGroup.Add(1)
	//p.waitGroup = &world.waitGroup
	go p.WorkerThreadExecutor()
}

func (world *World) assignWorkFunction(node int, name string) {
	function, ok := world.associates[name]
	if !ok {
		fmt.Println("Assigning function is not registered")
		return
	}
	p, ok := world.processMap[node]
	if !ok {
		fmt.Println("Process to assign function not found")
		return
	}
	p.workers = append(p.workers, function)
	//fmt.Println("Len p.workers", len(p.workers))
}

func (world *World) registerWorkFunction(name string, function workFunction) {
	world.associates[name] = function
}

func (world *World) launchTimer(duration time.Duration) {
	//world.waitGroup.Add(1)
	go func() {
		//now := time.Now()
		//defer world.waitGroup.Done()
		for {
			world.networkLayer.Send(-1, -1, "*TIME")
			/*if world.timeout != 0 && time.Now().After(now.Add(world.timeout)) {
				world.networkLayer.Send(-1, -1, "END")
				return
			}*/
			time.Sleep(duration)
		}
	}()
}

func (world *World) notifier(duration time.Duration) {
	//world.waitGroup.Add(1)
	go func() {
		//defer world.waitGroup.Done()
		for {
			for _, v := range world.processMap {
				v.messageQueue.cond.Signal()
			}
			time.Sleep(duration)
		}
	}()
}

func (world *World) parseConfig(name string) {
	file, err := os.Open(name)
	if err != nil {
		fmt.Println("Can't open file", err)
		return
	}
	defer func(file *os.File) {
		if err := file.Close(); err != nil {
			fmt.Println("Errors closing file", err)
			return
		}
	}(file)

	scanner := bufio.NewScanner(file)
	bidirectional := true
	for scanner.Scan() {
		fmt.Printf("%v\n", scanner.Text())

		var startProcess, endProcess, from, to, latency, timer, arg, sleepTime int
		latency = 1
		var name, msg string
		if _, ok := fmt.Sscanf(scanner.Text(), "bidirectional %v", &bidirectional); ok == nil {
		} else if strings.HasPrefix(scanner.Text(), ";") {
		} else if _, ok := fmt.Sscanf(scanner.Text(), "processes %v %v", &startProcess, &endProcess); ok == nil {
			for i := startProcess; i <= endProcess; i++ {
				world.createProcess(i)
			}
			world.notifier(notifierTic)
		} else if _, ok := fmt.Sscanf(scanner.Text(), "link from %v to %v latency %v", &from, &to, &latency); ok == nil {
			world.networkLayer.CreateLink(from, to, latency, bidirectional)
		} else if _, ok := fmt.Sscanf(scanner.Text(), "link from %v to %v", &from, &to); ok == nil {
			world.networkLayer.CreateLink(from, to, latency, bidirectional)
		} else if _, ok := fmt.Sscanf(scanner.Text(), "link from %v to all latency %v", &from, &latency); ok == nil {
			world.networkLayer.AddLinksToAll(from, latency, bidirectional)
		} else if _, ok := fmt.Sscanf(scanner.Text(), "link from %v to all latency %v", &from, &latency); ok == nil {
			world.networkLayer.AddLinksToAll(from, latency, bidirectional)
		} else if _, ok := fmt.Sscanf(scanner.Text(), "link from %v to all", &from); ok == nil {
			world.networkLayer.AddLinksToAll(from, latency, bidirectional)
		} else if _, ok := fmt.Sscanf(scanner.Text(), "link from all to %v latency %v", &to, &latency); ok == nil {
			world.networkLayer.AddLinksFromAll(to, latency, bidirectional)
		} else if _, ok := fmt.Sscanf(scanner.Text(), "link from all to %v", &to); ok == nil {
			world.networkLayer.AddLinksFromAll(to, latency, bidirectional)
		} else if _, ok := fmt.Sscanf(scanner.Text(), "link from all to all latency %d", &latency); ok == nil {
			world.networkLayer.AddLinksAllToAll(latency)
		} else if _, ok := fmt.Sscanf(scanner.Text(), "link from all to all"); ok == nil {
			world.networkLayer.AddLinksAllToAll(latency)
			//fmt.Printf("net map %v\n", world.networkLayer.networkMap)
		} else if _, ok := fmt.Sscanf(scanner.Text(), "setprocesses %v %v %v", &startProcess, &endProcess, &name); ok == nil {
			for i := startProcess; i <= endProcess; i++ {
				world.assignWorkFunction(i, name)
			}
		} else if _, ok := fmt.Sscanf(scanner.Text(), "send from %v to %v %v %v", &from, &to, &msg, &arg); ok == nil {
			world.networkLayer.Send(from, to, msg, arg)
		} else if _, ok := fmt.Sscanf(scanner.Text(), "send from %v to %v %v", &from, &to, &msg); ok == nil {
			world.networkLayer.Send(from, to, msg)
		} else if _, ok := fmt.Sscanf(scanner.Text(), "wait %v", &sleepTime); ok == nil {
			time.Sleep(time.Duration(sleepTime)*waitTic)
			//world.sleepTime = time.Duration(sleepTime)*time.Second
		} else if _, ok := fmt.Sscanf(scanner.Text(), "launch timer %v", &timer); ok == nil {
			world.launchTimer(time.Duration(timer)*timerTic)
		} else {
			fmt.Printf("unknown directive in input file: '%v'\n", scanner.Text())
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Scanner errors ", err)
	}
}

/*func (world World) Wait(){
	world.waitGroup.Wait()
}*/