package main

import (
	"fmt"
	"time"
)

func testFunction(process *Process, message Message) {
	//fmt.Println("testFunction")
	fmt.Println(process.node, message)
}

func Bully(process *Process, message Message) {
	if !message.IsMyMessage("Bully") { return }
	const ElectionWaitTime = time.Duration(5)*time.Second
	//fmt.Println(process.node, message)
	s := message.GetValue().(string)
	if s == "Bully_Election" {
		fmt.Printf("%3d: from %d: Start Election\n", process.node, message.From)
		if message.From>=0 {
			process.networkLayer.Send(process.node, message.From, "Bully_Alive")
			fmt.Printf("%3d: sent to %3d Alive\n", process.node, message.From)
		}
		if message.From < process.node {
			if !process.context.flagStarted {
				//find isMax
				isMax := process.node
				//fmt.Printf("%3d: net map = %v\n", process.node, process.networkLayer.networkMap)
				//fmt.Printf("%3d: neighbours = %v\n", process.node, process.Neighbours())
				for _, x := range process.Neighbours() {
					if x > isMax {
						isMax = x
					}
				}
				//fmt.Printf("%3d: isMax = %v\n", process.node, isMax)
				if isMax == process.node {
					process.context.flagStarted = true
					process.context.flagFinished = true
					for _, x := range process.Neighbours() {
						process.networkLayer.Send(process.node, x, "Bully_Coordinator")
					}
					fmt.Printf("%3d: I'm Coordinator\n", process.node)
				} else {
					process.context.flagStarted = true
					process.context.startTime = time.Now()
					for _, x := range process.Neighbours() {
						if x > process.node {
							process.networkLayer.Send(process.node, x, "Bully_Election")
							fmt.Printf("%3d: sent to %3d Election\n", process.node, x)
						}
					}
				}
			}
		}
	} else if s == "Bully_Alive" {
		process.context.flagFinished = true
	} else if s == "*TIME" {
		if process.context.flagStarted && !process.context.flagFinished && time.Now().Sub(process.context.startTime) > ElectionWaitTime {
			process.context.flagFinished = true
			for _, x := range process.Neighbours() {
				process.networkLayer.Send(process.node, x, "Bully_Coordinator")
			}
			fmt.Printf("%3d: I'm Coordinator\n", process.node)
		}
	} else if s == "Bully_Coordinator" {
		fmt.Printf("%3d: %3d is Coordinator\n", process.node, message.From)
		//process.context.debugCount++
	} else {
		fmt.Printf("%3d: Something strange\n", process.node)
	}
}

func main() {
	/*m := make(map[int] map [int] int)
	m[5] = make(map[int]int)
	m[5][6] = 3
	fmt.Printf("map %v\n", m)
	world :=NewWorld()
	world.networkLayer.CreateLink(5, 3, 4, true)

	fmt.Println(world.networkLayer.networkMap)
	//world.networkLayer.threads = append(world.networkLayer.threads, 2)
	var p1, p2 Process
	world.networkLayer.registerProcess(2, &p1)
	world.networkLayer.registerProcess(3, &p2)
	//world.createProcess(2)
	//world.createProcess(3)
	fmt.Println(world.networkLayer.threads)
	return*/

	world := NewWorld()
	world.registerWorkFunction("TEST", testFunction)
	world.registerWorkFunction("BULLY", Bully)
	world.parseConfig("config.data")
	time.Sleep(20*time.Second)
	//world.Wait()
	return
}
