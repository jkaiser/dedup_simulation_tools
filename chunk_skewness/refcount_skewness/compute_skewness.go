/*
   This program computes the chunk skewness, i.e. the number of chunks that occur exactly N times.
   Example:
       - Chunk A occurs 2 times
       - Chunk B occurs 1 time
       - Chunk C occurs 2 times
       - Chunk D occurs 2 times

   Then the output would be:
   1,1
   2,3

   Interpretation: There is one chunk that occured exactly one time. There are 3 chunks that occur exactly 2 times.
*/
package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
)
import algocommon "github.com/jkaiser/dedup_simulations/dedupAlgorithms/common"
import log "github.com/cihub/seelog"

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

type Results struct {
	NumChunksZero uint64
	NumChunksOne  uint64
	MostUsedChunk string
}

func (r *Results) Add(that *Results) {
	r.NumChunksZero += that.NumChunksZero
	r.NumChunksOne += that.NumChunksOne
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

func writeResults(refCounter []int32, resultsFile string) {

	f, err := os.OpenFile(resultsFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Error("couldn't open output file: ", err)
		return
	}
	defer f.Close()

	for i, cnt := range refCounter {
		if cnt != 0 {
			fmt.Fprintf(f, "%v,%v\n", i, cnt)
		}
	}
}

func writeResultsZeroOne(results Results, resultsFile string) {

	f, err := os.OpenFile(resultsFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Error("couldn't open output file: ", err)
		return
	}
	defer f.Close()

	if encodedStats, err := json.MarshalIndent(results, "", "    "); err != nil {
		log.Error("Couldn't marshal results: ", err)
	} else {
		log.Info("results: ")
		log.Info(bytes.NewBuffer(encodedStats).String())
		f.Write(encodedStats)
	}
}

func processFile(inFile string, chunkIndex map[[12]byte]int32, specialChunks map[string]string) (map[[12]byte]int32, Results) {

	var res Results
	var chunkHashBuf [12]byte // Used to make the Digest useable in maps.
	fileEntryChan := make(chan *algocommon.FileEntry, algocommon.ConstMaxFileEntries)

	// setup zero and one-chunks
	var zeroChunk, oneChunk []byte
	var err error
	zeroChunk, err = hex.DecodeString(specialChunks["zero"])
	if err != nil {
		panic(err)
	}
	oneChunk, err = hex.DecodeString(specialChunks["one"])
	if err != nil {
		panic(err)
	}

	// generate traceDataReader
	log.Debug("generate TraceDataReader for path: ", inFile)
	tReader := algocommon.NewTraceDataReader(inFile)
	go tReader.FeedAlgorithm(fileEntryChan)

	// perform the refcounting
	for fileEntry := range fileEntryChan {

        log.Info("processing traced file ", fileEntry.TracedFile.Filename)
		if !strings.HasSuffix(fileEntry.TracedFile.Filename, "dmtcp") {
			log.Debugf("skip traced file %v", fileEntry.TracedFile.Filename)
			tReader.GetFileEntryReturn() <- fileEntry
			continue
		}

		for i := range fileEntry.Chunks {
			copy(chunkHashBuf[:], fileEntry.Chunks[i].Digest)
			if numOcc, ok := chunkIndex[chunkHashBuf]; ok { // old entry
				chunkIndex[chunkHashBuf] = numOcc + 1
			} else {
				chunkIndex[chunkHashBuf] = 1
			}

			// check for special chunks
			if bytes.Equal(chunkHashBuf[:], zeroChunk) {
				res.NumChunksZero++
			} else if bytes.Equal(chunkHashBuf[:], oneChunk) {
				res.NumChunksOne++
			}
		}
		tReader.GetFileEntryReturn() <- fileEntry
	}

	return chunkIndex, res
}

func computeSkewness(inFiles []string) ([]int32, Results) {

	var specialChunksMap map[string]map[string]string = make(map[string]map[string]string)
	specialChunksMap["cdc4"] = map[string]string{"zero": "897256b6709e1a4da9daba92", "one": "95e00e7bbef9a74788304629"}
	specialChunksMap["cdc8"] = map[string]string{"zero": "5188431849b4613152fd7bdb", "one": "04f90e279f910e4823b29054"}
	specialChunksMap["cdc16"] = map[string]string{"zero": "1adc95bebe9eea8c112d40cd", "one": "174d9c9e92d4e03045df6bad"}
	specialChunksMap["fixed2"] = map[string]string{"zero": "605db3fdbaff4ba13729371a", "one": "e6333e53570fb05a841a7f14"}
	specialChunksMap["fixed4"] = map[string]string{"zero": "1ceaf73df40e531df3bfb26b", "one": "e0c66649d1434eca3435033a"}
	specialChunksMap["fixed8"] = map[string]string{"zero": "0631457264ff7f8d5fb1edc2", "one": "5e2b96c19c4f5c63a5afa2de"}
	specialChunksMap["fixed16"] = map[string]string{"zero": "897256b6709e1a4da9daba92", "one": "547372f1044a3442aa52fcd2"}
	specialChunksMap["fixed32"] = map[string]string{"zero": "5188431849b4613152fd7bdb", "one": "ca711c69165e1fa5be72993b"}

	var res, tmpResult Results

	var chunkIndex map[[12]byte]int32 = make(map[[12]byte]int32, 1e6)
	for _, inFile := range inFiles {
		specialChMapToUse := specialChunksMap["cdc8"] // cdc8 per default
		for k, v := range specialChunksMap {
			if strings.Contains(inFile, k) {
				specialChMapToUse = v
				break
			}
		}
		log.Infof("Start processing file %v. So far we have %v different chunks", inFile, len(chunkIndex))
		chunkIndex, tmpResult = processFile(inFile, chunkIndex, specialChMapToUse)
		res.Add(&tmpResult)
	}

	var max int32
	for _, refCnt := range chunkIndex {
		if refCnt > max {
			max = refCnt
		}
	}

	refs := make([]int32, max+1)

	for _, refCnt := range chunkIndex {
		refs[refCnt]++
	}

	// identify the most used chunk if possible
	if uint64(max) == res.NumChunksZero {
		res.MostUsedChunk = "zero"
	} else if uint64(max) == res.NumChunksOne {
		res.MostUsedChunk = "one"
	} else {
		res.MostUsedChunk = "unkown"
	}

	return refs, res
}

func main() {
	runtime.GOMAXPROCS(2)
	defer log.Flush()
	var descr string = `   This program computes the chunk skewness, i.e. the number of chunks that occur exactly N times.
   Example:
       - Chunk A occurs 2 times
       - Chunk B occurs 1 time
       - Chunk C occurs 2 times
       - Chunk D occurs 2 times

   Then the output would be:
   1,1
   2,3

   Interpretation: There is one chunk that occured exactly one time. There are 3 chunks that occur exactly 2 times.
`
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s\nUsage of %s:\n", descr, os.Args[0])
		flag.PrintDefaults()
	}
	in_files := flag.String("traces", "", "The COMMA-SEPERATED list of trace files to consider.")
	resultsFile := flag.String("out", "out", "The output file.")
	zeroOneOutFile := flag.String("zeroOneStats", "zeroOneStats.json", "The output file for the zero-chunk/one-chunk statistics.")

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

	inputFiles := computeListOfFiles(*in_files)
	if len(inputFiles) == 0 {
		panic("Found no valid input file in the list of given ones.")
	}

	refs, zeroOneStats := computeSkewness(inputFiles)

	writeResults(refs, *resultsFile)
	writeResultsZeroOne(zeroOneStats, *zeroOneOutFile)
}
