package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

type converter struct {
	timeRef        int
	jobs           map[string]bool
	eventsToIgnore map[string]bool
}

func newConverter() *converter {
	var c converter
	c.jobs = make(map[string]bool)
	c.eventsToIgnore = make(map[string]bool)
	return &c
}

type dagEvent struct {
	timestamp       int
	job             string
	id              string
	eventComplement string
}

func (c *converter) ignoreEvents(events string) {
	parts := strings.Split(events, ",")
	for _, e := range parts {
		c.eventsToIgnore[e] = true
	}
}

func (c *converter) convert(line string) (string, error) {
	event, err := parseDagLog(line)

	if err != nil {
		return "", err
	}

	if c.timeRef == 0 {
		c.timeRef = event.timestamp
	}

	job := event.job
	eventID := event.id
	if eventID == "***" {
		eventID = event.eventComplement
		job = "DAG"
	}

	if _, ok := c.eventsToIgnore[eventID]; ok {
		return "", errors.New("Ignoring event")
	}

	result := fmt.Sprintf("@%d\n%s is %s", event.timestamp-c.timeRef, job, eventID)
	if _, ok := c.jobs[job]; !ok {
		result = fmt.Sprintf("concise \"%s\" as %s\n%s", job, job, result)
		c.jobs[job] = true
	}

	return result, nil
}

func parseDagLog(logLine string) (dagEvent, error) {
	parts := strings.SplitN(logLine, " ", 5)

	if len(parts) < 5 {
		errmsg := fmt.Sprintf("' [WARN] skipping line: %s", logLine)
		log.Printf(errmsg)
		return dagEvent{}, errors.New(errmsg)
	}

	timestamp, err := strconv.Atoi(parts[0])
	if err != nil {
		errmsg := fmt.Sprintf("' [WARN] skipping line (unable to parse timestamp): %s", logLine)
		log.Printf(errmsg)
		return dagEvent{}, errors.New(errmsg)
	}

	return dagEvent{timestamp: timestamp, job: parts[1], id: parts[2], eventComplement: parts[3]}, nil
}

func main() {
	inputFile := flag.String("in", "", "input filepath")
	ignoreEvents := flag.String("ignore", "", "comma separated list of events to ignore, ex. IMAGE_SIZE,JOB_SUCCESS")
	flag.Parse()

	if *inputFile == "" {
		log.Fatal("Please set the input filepath.")
	}

	file, err := os.Open(*inputFile)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	cvtr := newConverter()
	cvtr.ignoreEvents(*ignoreEvents)

	fmt.Println("@startuml")
	for scanner.Scan() {
		line, err := cvtr.convert(scanner.Text())
		if err != nil {
			continue
		}
		fmt.Println(line)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("@endtuml")
}
