package raft

import (
	"log"
	"math/rand"
)

// Debugging
// const Debug = true
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func GetRandomElectTimeOut(rd *rand.Rand) int {
	plusMs := int(rd.Float64() * 150)
	return plusMs + ElectTimeOutBase
}
