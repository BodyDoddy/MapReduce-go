package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"os"
	"sort"
)

func runMapTask(
	jobName string, // The name of the whole mapreduce job
	mapTaskIndex int, // The index of the map task
	inputFile string, // The path to the input intermfile assigned to this task
	nReduce int, // The number of reduce tasks that will be run
	mapFn func(intermfile string, contents string) []KeyValue, // The user-defined map function
) {

	contents, err := os.ReadFile(inputFile)
	if err != nil {
		panic(err)

	}
	keyValues := mapFn(inputFile, string(contents))
	intermfiles := make(map[string]*os.File)
	for _, keyval := range keyValues {
		hashval := int(hash32(keyval.Key)) % nReduce
		fileName := getIntermediateName(jobName, mapTaskIndex, hashval)
		if _, ok := intermfiles[fileName]; !ok {
			intermfile, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
			if err != nil {
				panic(err)
			}
			intermfiles[fileName] = intermfile
			defer intermfile.Close()
		}
		//
		enc := json.NewEncoder(intermfiles[fileName])
		err = enc.Encode(&keyval)
		if err != nil {
			panic(err)
		}
	}
}

func hash32(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func runReduceTask(
	jobName string, // the name of the whole MapReduce job
	reduceTaskIndex int, // the index of the reduce task
	nMap int, // the number of map tasks that were run
	reduceFn func(key string, values []string) string,
) {

	keyvalues := make(map[string][]string)
	for mapIndex := range make([]int, nMap) {
		fileName := getIntermediateName(jobName, mapIndex, reduceTaskIndex)
		if intermfile, err := os.Open(fileName); err != nil {
			panic(err)

		} else {
			defer intermfile.Close()
			decoder := json.NewDecoder(intermfile)
			for {
				var keyvalue KeyValue
				err := decoder.Decode(&keyvalue)
				if err != nil {
					break
				}
				keyvalues[keyvalue.Key] = append(keyvalues[keyvalue.Key], keyvalue.Value)
			}
		}
	}

	keys := []string{}
	for k := range keyvalues {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fileName := getReduceOutName(jobName, reduceTaskIndex)
	outFile, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		panic(err)
	}
	defer outFile.Close()
	encoder := json.NewEncoder(outFile)
	for _, key := range keys {
		keyval := KeyValue{key, reduceFn(key, keyvalues[key])}
		err := encoder.Encode(&keyval)
		if err != nil {
			panic(err)
		}
	}
}
