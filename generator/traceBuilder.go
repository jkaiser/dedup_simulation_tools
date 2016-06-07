package main

import "bufio"
import "io"
import "os"
import "os/exec"
import "path"
import "io/ioutil"
import log "github.com/cihub/seelog"
import "github.com/gogo/protobuf/proto"
import "github.com/jkaiser/dedup_tools/parser"

func writeFull(w io.Writer, d []byte) error {
	var tw int = len(d)
	var written int

	for written < tw {
		if n, err := w.Write(d); err != nil {
			return err
		} else {
			written += n
			d = d[n:]
		}
	}

	return nil
}

func WriteMessage(toWrite <-chan []byte, path string, closeSignal chan<- bool) {

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		log.Error("couldn't open output file: ", err)
		closeSignal <- false
		return
	}

	bufWriter := bufio.NewWriterSize(f, 4*1024*1024)

	defer f.Close()

	for m := range toWrite {
		varintBuf := proto.EncodeVarint(uint64(len(m)))
		if err = writeFull(bufWriter, varintBuf); err != nil {
			panic(err)
		}
		if err = writeFull(bufWriter, m); err != nil {
			panic(err)
		}
	}

	bufWriter.Flush()
	closeSignal <- true
}

func createSingleTrace(dp PlanForDay, doneChan chan bool) {

	if _, err := os.Stat(dp.TargetFile); err == nil {
		os.Remove(dp.TargetFile)
	}

	var tempDir string
	var err error
	if tempDir, err = ioutil.TempDir(os.TempDir(), "multiTraceGeneration"); err != nil {
		log.Error("Couldn't create temporary directory:", err)
		doneChan <- false
		return
	}

	for _, source := range dp.SourceFiles {

		// unzip
		tmpTarget := path.Join(tempDir, path.Base(source))
		cmd := exec.Command("cp", source, tempDir)
		if _, err := cmd.Output(); err != nil {
			log.Error("Couldn't copy ", source, " to ", tempDir, " :", err)
			doneChan <- false
			return
		}

		cmd = exec.Command("gzip", "-f", "-d", tmpTarget)
		if _, err := cmd.Output(); err != nil {
			log.Error("Couldn't unzip ", tmpTarget, " :", err)
			doneChan <- false
			return
		}

		tmpTarget = tmpTarget[:len(tmpTarget)-3] // remove the ".gz"

		log.Debug("will parse ", tmpTarget, " to ", dp.TargetFile)
		pbufChan := make(chan []byte, 10000)
		closeChan := make(chan bool)
		go WriteMessage(pbufChan, dp.TargetFile, closeChan)
		ubcParser := parser.NewUBCParser(tmpTarget, pbufChan)
		go ubcParser.ParseFile()
		<-closeChan

		os.Remove(tmpTarget)
	}

	if err = os.RemoveAll(tempDir); err != nil {
		log.Warn("Couldn't remove temporary directory ", tempDir, " :", err)
	}
	doneChan <- true
}

func buildTraces(plan *OutputJSON, maxConcurrentTasks int) {
	runningTasks := 0
	doneChan := make(chan bool, 100)

	for _, plansForDay := range plan.Plan {

		for i := range plansForDay {
			if runningTasks < maxConcurrentTasks {
				go createSingleTrace(plansForDay[i], doneChan)
				runningTasks++
			} else {
				<-doneChan
				go createSingleTrace(plansForDay[i], doneChan)
			}
		}
	}

	for runningTasks != 0 {
		<-doneChan
		runningTasks--
		log.Debug("joined build routine")
	}

}
