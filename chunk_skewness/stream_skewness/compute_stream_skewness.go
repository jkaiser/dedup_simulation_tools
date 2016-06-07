/*
   This program computes the stream skewness, i.e. how many chunks occur in exaclty  N streams.
   Example:
       - Chunk A occurs int 2 streams
       - Chunk B occurs 1 stream
       - Chunk C occurs 2 streams
       - Chunk D occurs 2 streams

   Then the output would be:
   1,1
   2,3

   Interpretation: There is one chunk that occured exactly in one stream. There are 3 chunks that occur exactly 2 streams.
*/
package main

import (
	"flag"
	"fmt"
	"os"
	"path"
	"runtime"
	"runtime/pprof"
	"strings"
)
import algocommon "github.com/jkaiser/dedup_simulations/dedupAlgorithms/common"
import log "github.com/cihub/seelog"

type StreamStats struct {
	volume              int64
	numberOfOccurrences uint32
	//lastStream int16
}

func setupLogger(debug bool) {
	var testConfig string
	if debug {
		testConfig = `
<seelog type="sync">
    <outputs formatid="main">
        <filter levels="debug">
            <console/>
        </filter>
        <filter levels="info">
            <console/>
        </filter>
        <filter levels="error">
            <console/>
        </filter>
        <filter levels="warn">
            <console/>
        </filter>
        <filter levels="critical">
            <console/>
        </filter>
    </outputs>
    <formats>
        <format id="main" format="%Date %Time [%Level] %Msg%n"/>
    </formats>
</seelog>`

	} else {
		testConfig = `
<seelog type="sync">
    <outputs formatid="main">
        <filter levels="info">
            <console/>
        </filter>
        <filter levels="error">
            <console/>
        </filter>
        <filter levels="warn">
            <console/>
        </filter>
        <filter levels="critical">
            <console/>
        </filter>
    </outputs>
    <formats>
        <format id="main" format="%Date %Time [%Level] %Msg%n"/>
    </formats>
</seelog>`
	}

	if logger, err := log.LoggerFromConfigAsBytes([]byte(testConfig)); err != nil {
		fmt.Println(err)
	} else {
		if loggerErr := log.ReplaceLogger(logger); loggerErr != nil {
			fmt.Println(loggerErr)
		}
	}
}

func computeListOfFiles(strFiles string) []string {

	validFiles := make([]string, 0)
	f := strings.Split(strFiles, ",")

	for i := range f {
		if stat, err := os.Stat(f[i]); err != nil {
			panic(err)
		} else if !stat.Mode().IsRegular() {
			panic("One of the given input files isn't a regular file: " + stat.Name())
		}
		validFiles = append(validFiles, f[i])
	}

	return validFiles
}

func writeResults(refCounter []int32, refOccurrences []uint32, refVolumes []int64, resultsFile string) {

	f, err := os.OpenFile(resultsFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Error("couldn't open output file: ", err)
		return
	}
	defer f.Close()

	for i, cnt := range refCounter {
		if cnt != 0 {
			fmt.Fprintf(f, "%v,%v,%v,%v\n", i, cnt, refOccurrences[i], refVolumes[i])
		}
	}
}

func processFile(inFile string, chunkIndex map[[12]byte]map[string]*StreamStats) map[[12]byte]map[string]*StreamStats {

	var chunkHashBuf [12]byte // Used to make the Digest useable in maps.
	fileEntryChan := make(chan *algocommon.FileEntry, algocommon.ConstMaxFileEntries)

	// generate traceDataReader
	log.Debug("generate TraceDataReader for path: ", inFile)
	tReader := algocommon.NewTraceDataReader(inFile)
	go tReader.FeedAlgorithm(fileEntryChan)

	for fileEntry := range fileEntryChan {

		if !strings.HasSuffix(fileEntry.TracedFile.Filename, "dmtcp") {
			log.Debugf("skip traced file %v", fileEntry.TracedFile.Filename)
			tReader.GetFileEntryReturn() <- fileEntry
			continue
		}

		var streamID string = path.Base(fileEntry.TracedFile.Filename)

		for i := range fileEntry.Chunks {
			copy(chunkHashBuf[:], fileEntry.Chunks[i].Digest)

			if ciEntry, ok := chunkIndex[chunkHashBuf]; ok { // old entry

				if sstats, ok := ciEntry[streamID]; ok {
					sstats.numberOfOccurrences++
					sstats.volume += int64(fileEntry.Chunks[i].Size)
				} else {
					ciEntry[streamID] = &StreamStats{volume: int64(fileEntry.Chunks[i].Size), numberOfOccurrences: 1}
				}
			} else {
				m := make(map[string]*StreamStats)
				m[streamID] = &StreamStats{volume: int64(fileEntry.Chunks[i].Size), numberOfOccurrences: 1}
				chunkIndex[chunkHashBuf] = m
			}
		}

		tReader.GetFileEntryReturn() <- fileEntry
	}

	return chunkIndex
}

func computeSkewness(inFiles []string) ([]int32, []uint32, []int64) {

	chunkIndex := make(map[[12]byte]map[string]*StreamStats, 1e6) // holds for each fp a map. This map contains all streamIDs of all streams which contain that chunk/fp
	for _, file := range inFiles {
		log.Infof("Start processing file %v. So far we have %v different chunks", file, len(chunkIndex))
		chunkIndex = processFile(file, chunkIndex)
	}

	var max int
	for _, ciEntry := range chunkIndex {
		if len(ciEntry) > max {
			max = len(ciEntry)
		}
	}

	// count the number of occurences
	streamCounters := make([]int32, max+1)     // counts the number of different chunks that occur in x streams
	streamOccurrences := make([]uint32, max+1) // counts the number of occurrences of chunks that occur in x streams. The difference to "streamCounters" is that  "streamCounters" counts each unique chunk onnly once while "streamOccurrences" counts every occurence of these chunks
	streamVolumes := make([]int64, max+1)      // similar to "streamOccurrences", just that it counts the combined volume of all chunks that occur x times

	for _, ciEntry := range chunkIndex {
		streamCounters[len(ciEntry)]++
		for _, sstat := range ciEntry {
			streamVolumes[len(ciEntry)] += sstat.volume
			streamOccurrences[len(ciEntry)] += sstat.numberOfOccurrences
		}
	}

	return streamCounters, streamOccurrences, streamVolumes
}

// This program will look for fileentries in the given trace files and only consider those which end with "TODO".
func main() {
	runtime.GOMAXPROCS(2)
	defer log.Flush()

	var descr string = `   This program computes the stream skewness, i.e. how many chunks occur in exaclty  N streams.
   Example:
       - Chunk A occurs int 2 streams
       - Chunk B occurs 1 stream
       - Chunk C occurs 2 streams
       - Chunk D occurs 2 streams

   Then the output would be:
   1,1
   2,3

   Interpretation: There is one chunk that occured exactly in one stream. There are 3 chunks that occur exactly 2 streams.
`
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s\nUsage of %s:\n", descr, os.Args[0])
		flag.PrintDefaults()
	}

	in_files := flag.String("traces", "", "The COMMA-SEPERATED list of trace files to consider.")
	resultsFile := flag.String("out", "out", "The output file.")

	debug := flag.Bool("debug", false, "Enables full debug output.")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	/*memprofile := flag.String("memprofile", "", "write memory profile to this file")*/
	/*liveprofile := flag.Bool("liveprofile", false, "Provide a live profile (CPU + Mem + Block). See http://blog.golang.org/profiling-go-programs for details.")*/
	flag.Parse()

	setupLogger(*debug)

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Critical(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	fileList := computeListOfFiles(*in_files)
	if len(fileList) == 0 {
		panic("Found no valid input file in the list of given ones.")
	}

	refs, refsOccurrences, refsVolumes := computeSkewness(fileList)

	writeResults(refs, refsOccurrences, refsVolumes, *resultsFile)
}
