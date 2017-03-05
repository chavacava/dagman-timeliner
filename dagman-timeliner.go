package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	converter "./converter"
)

func main() {
	inputFile := flag.String("in", "", "filepath of the DAG jobstate_log")
	outputFile := flag.String("out", "", "output filepath (defaults to stdout)")
	ignoreEvents := flag.String("events-i", "", "comma separated list of events to ignore, ex. IMAGE_SIZE,JOB_SUCCESS")
	decorateEvents := flag.Bool("events-deco", true, "decorate events with information on total time of events")
	flag.Parse()

	if *inputFile == "" {
		log.Fatal("Please set the input filepath.")
	}

	file, err := os.Open(*inputFile)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	var out io.Writer
	if *outputFile == "" {
		out = os.Stdout
	} else {
		ofile, err := os.OpenFile(
			*outputFile,
			os.O_WRONLY|os.O_TRUNC|os.O_CREATE,
			0666,
		)
		if err != nil {
			log.Fatalf("Unable to create the file %s", *outputFile)
		}
		defer ofile.Close()
		out = ofile
	}

	config := converter.NewConfiguration()
	config.IgnoreEvents(*ignoreEvents)
	config.DecorateEvents(*decorateEvents)
	cvtr := converter.NewConverter(*config)
	scanner := bufio.NewScanner(file)

	fmt.Fprintln(out, "@startuml")
	for scanner.Scan() {
		line, err := cvtr.Convert(scanner.Text())
		if err != nil {
			continue
		}
		fmt.Fprintln(out, line)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	fmt.Fprintln(out, "@enduml")
}
