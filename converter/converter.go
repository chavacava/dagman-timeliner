package converter

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
)

// Converter from DAGMan log to PlantUML timeline
type Converter struct {
	timeRef int
	// Map job->last event timestamp
	jobs map[string]int
	conf Configuration
}

// Configuration of Converter
type Configuration struct {
	eventsToIgnore map[string]bool
	decoEvents     bool
}

type dagEvent struct {
	timestamp       int
	job             string
	id              string
	eventComplement string
}

// NewConfiguration yields a Converter configuration
func NewConfiguration() *Configuration {
	c := Configuration{eventsToIgnore: make(map[string]bool), decoEvents: true}
	return &c
}

// NewConverter yields a converter with the given configuration.
func NewConverter(config Configuration) *Converter {
	var c Converter
	c.jobs = make(map[string]int)
	c.conf = config
	return &c
}

// IgnoreEvents configures events to ignore
func (c *Configuration) IgnoreEvents(events string) {
	parts := strings.Split(events, ",")
	for _, e := range parts {
		c.eventsToIgnore[e] = true
	}
}

// DecorateEvents configures if events should be decorated
func (c *Configuration) DecorateEvents(d bool) {
	c.decoEvents = d
}

// Convert DAGMan log into PlantUML timeline
func (c *Converter) Convert(line string) (string, error) {
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

	if _, ok := c.conf.eventsToIgnore[eventID]; ok {
		return "' Ignoring: " + line, nil
	}

	result := fmt.Sprintf("@%d\n%s is %s", event.timestamp-c.timeRef, job, eventID)
	if t, ok := c.jobs[job]; !ok {
		result = fmt.Sprintf("concise \"%s\" as %s\n%s", job, job, result)
	} else if c.conf.decoEvents && event.timestamp-t > 0 {
		result = fmt.Sprintf("%s@%d <-> @%d : %ds\n%s", job, t-c.timeRef, event.timestamp-c.timeRef, event.timestamp-t, result)
	}
	c.jobs[job] = event.timestamp

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
