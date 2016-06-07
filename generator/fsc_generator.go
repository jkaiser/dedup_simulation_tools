package main

import "fmt"
import "flag"
import "os"
import "path"
import "path/filepath"
import "io/ioutil"
import "time"
import "sort"
import "strconv"
import "encoding/json"
import "crypto/sha1"
import "math/rand"
import "runtime/pprof"

import log "github.com/cihub/seelog"

// The output scheme for the output json file.
type OutputJSON struct {
	Config GeneratorConfig
	Plan   map[string][]PlanForDay
}

// a struct to hold the used configuration parameters
type GeneratorConfig struct {
	Seed         int64
	NumNodes     int
	NumStreams   int
	TraceRun     string
	MetaInfoHash string
}

// This is the plan (and build instruction) for a single stream for a single day
type PlanForDay struct {
	TargetFile  string
	SourceFiles []string
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

// generates the whole build plan for the traces
func generatePlan(seed int64, numNodes int, numStreams int, msSourceDir, suffix, targetDir string, traces []*MSTraceFile) *OutputJSON {

	addRelativeTime(traces)

	hostList := make([]string, 0, len(traces))
	hostSet := make(map[string]bool, len(traces))
	for i := range traces {
		if hostSet[traces[i].Hostname] {
			hostList = append(hostList, traces[i].Hostname)
		}
	}

	// build generatorConfig
	config := &GeneratorConfig{NumNodes: numNodes, NumStreams: numStreams}

	// setup random generator
	var rng *rand.Rand
	if seed == 0 {
		config.Seed = time.Now().Unix()
		rng = rand.New(rand.NewSource(config.Seed))
	} else {
		config.Seed = seed
		rng = rand.New(rand.NewSource(seed))
	}

	// choose nodes
	perm := rng.Perm(len(traces))
	chosenHosts := make([]string, numNodes)
	chosenSet := make(map[string]bool)
	for i := 0; i < numNodes; i++ {
		chosenHosts[i] = traces[perm[i]].Hostname
		chosenSet[traces[perm[i]].Hostname] = true
	}

	chosenTraces := make([]*MSTraceFile, 0)
	for i := range traces {
		if chosenSet[traces[i].Hostname] {
			chosenTraces = append(chosenTraces, traces[i])
		}
	}

	log.Debug("num of chosen traces: ", len(chosenTraces))
	allPlans := generatePlanPerDay(chosenTraces, msSourceDir, numStreams, suffix, targetDir)

	// build output JSON object
	metaOutput := new(OutputJSON)
	metaOutput.Config = *config
	metaOutput.Plan = make(map[string][]PlanForDay)
	for d := range allPlans {
		metaOutput.Plan[strconv.Itoa(d)] = allPlans[d]
	}

	return metaOutput
}

func generatePlanPerDay(chosenTraces []*MSTraceFile, msSourceDir string, numStreams int, suffix string, targetDir string) map[int][]PlanForDay {
	log.Info("create daily plans...")

	hostSet := make(map[string]bool)
	plansPerDayPerHost := make(map[int]map[string][]string)

	for _, trace := range chosenTraces {
		daysSinceEarliest := int(trace.diffToMin.Hours()) / 24
		hostSet[trace.Hostname] = true

		if p, ok := plansPerDayPerHost[daysSinceEarliest]; !ok { // new entry for this day
			plansPerDayPerHost[daysSinceEarliest] = make(map[string][]string)
			tmplist := make([]string, 0)
			tmplist = append(tmplist, path.Join(msSourceDir, trace.TraceFile))
			plansPerDayPerHost[daysSinceEarliest][trace.Hostname] = tmplist

		} else { // day is known
			if _, ok := p[trace.Hostname]; !ok { // but the host in this day is new
				p[trace.Hostname] = make([]string, 0)
				p[trace.Hostname] = append(p[trace.Hostname], path.Join(msSourceDir, trace.TraceFile))
			} else {
				p[trace.Hostname] = append(p[trace.Hostname], path.Join(msSourceDir, trace.TraceFile))
			}
		}
	}

	log.Debug("num days: ", len(plansPerDayPerHost))

	// compute which hosts should appear in which stream
	// each stream should have same amount of hosts. Additionally, the hosts should not switch among
	// streams.
	streamAssignment := make(map[string]int)
	streamCounter := 0
	for host := range hostSet {
		streamAssignment[host] = streamCounter
		streamCounter = (streamCounter + 1) % numStreams
	}
	if len(streamAssignment) != numStreams {
		log.Warn("Could create only ", len(streamAssignment), " streams with ", len(hostSet), " hosts")
	}

	log.Debug("num hosts: ", len(streamAssignment))

	// build the building plans for all days
	cnt := 0
	allPlans := make(map[int][]PlanForDay)
	for day, hostmap := range plansPerDayPerHost {

		// it is possible that not every host appears on each day -> there might a different number of streams per day
		streamMapForCurrentDay := make(map[int]*PlanForDay)
		for host, traceFiles := range hostmap {
			cnt += len(traceFiles)
			if pfd, ok := streamMapForCurrentDay[streamAssignment[host]]; !ok {
				// build new plan for the day
				nplan := new(PlanForDay)
				nplan.TargetFile, _ = filepath.Abs(path.Join(targetDir, fmt.Sprintf("gen_%v_stream%v%v", day, streamAssignment[host], suffix)))
				nplan.SourceFiles = traceFiles

				streamMapForCurrentDay[streamAssignment[host]] = nplan

			} else { // extend the sourcefiles list
				pfd.SourceFiles = append(pfd.SourceFiles, traceFiles...)
			}
		}

		allPlans[day] = make([]PlanForDay, 0)
		for _, planForStream := range streamMapForCurrentDay {
			allPlans[day] = append(allPlans[day], *planForStream)
		}
	}

	log.Debug("total tracefiles chosen: ", cnt)

	// sort the input names
	for d, dayplan := range allPlans {
		log.Debug("num plans for day ", d, ": ", len(dayplan))
		for _, pfd := range dayplan {
			a := sort.StringSlice(pfd.SourceFiles)
			a.Sort()
			pfd.SourceFiles = a
			log.Debug("sorted files: ", pfd.SourceFiles)
		}
	}

	// debug output
	/*highestDay := 0*/
	/*for day := range allPlans {*/
	/*if day > highestDay {*/
	/*highestDay = day*/
	/*}*/
	/*}*/

	/*for i := 0; i <= highestDay; i++ {*/
	/*if plan, ok := allPlans[i]; ok {*/
	/*log.Debug("num traces for day ", i, ": ", len(plan.SourceFiles))*/
	/*} else {*/
	/*log.Debug("num traces for day ", i, ": 0")*/
	/*}*/
	/*}*/

	return allPlans
}

// Converts the microsoft traces from Meyer et al. to fs-c traces.
// Example call: To create a trace consisting of 64 randomly chosen nodes, which are distributed on 20 streams:
// GOMAXPROCS=16 ./generator -s 20 -n 64 -seed 62034310 -data_dir /project/zdvresearch/shared_ecs/deduplication/microsoftTraces/var/www/traces/UBC-Dedup/8rb -out /project/zdvresearch/shared_ecs/deduplication/manyStreams/big_hostset_traces/64hosts/multi_20streams_62034310
func main() {
	defer log.Flush()

	data_dir := flag.String("data_dir", "", "The directory containing the trace files.")
	metainfoFile := flag.String("meta", "all_file_metadata.txt", "The input metainfo file.")
	resultsDirectory := flag.String("out", "fscTraceOut", "The output directory.")
	traceRun := flag.String("trace", "8rb", "The specific tracerun (8rb, 8r, 16rb, ...) of the microsoft traces to generate the fs-c input from.")
	suffix := flag.String("suffix", "", "Suffix of each fsc output file.")
	numNodes := flag.Int("n", 1, "The number of randomly chosen nodes.")
	numStreams := flag.Int("s", 0, "The maximum number of streams per week. The nodes will stay in one trace, so there might be weeks that have less traces than available. [default: numNodes]")
	seed := flag.Int64("seed", 0, "The seed for the internal PRNG.")

	debug := flag.Bool("debug", false, "Enables full debug output.")
	sim := flag.Bool("sim", false, "Just create buildplan.")
	maxParallelConversions := flag.Int("maxParallel", 8, "Number of parallel trace generations.")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	memprofile := flag.String("memprofile", "", "write memory profile to this file")
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

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Critical(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
		return
	}

	// input sanity checks
	if _, err := os.Stat(*data_dir); os.IsNotExist(err) {
		log.Error("Trace source directory doesn't exist")
		return
	}

	// start
	traces := loadMetadata(*metainfoFile, *traceRun)
	if len(traces) == 0 {
		log.Error("No tracefiles found in metadata")
		return
	}

	var nStreams int = *numNodes
	if *numStreams > 0 {
		nStreams = *numStreams
	}
	plan := generatePlan(*seed, *numNodes, nStreams, *data_dir, *suffix, *resultsDirectory, traces)
	plan.Config.TraceRun = *traceRun
	buf, _ := ioutil.ReadFile(*metainfoFile)
	plan.Config.MetaInfoHash = fmt.Sprintf("%x", sha1.Sum(buf))

	// create and cleanup outputdir if necessary
	os.RemoveAll(*resultsDirectory)
	if err := os.Mkdir(*resultsDirectory, 0755); err != nil {
		log.Error("Couldn't create output directory")
		return
	}

	// write output json
	if encodedStats, err := json.MarshalIndent(plan, "", "    "); err != nil {
		log.Error("Couldn't marshal statistics: ", err)
	} else if f, err := os.OpenFile(path.Join(*resultsDirectory, "plan.txt"), os.O_WRONLY|os.O_CREATE, 0644); err != nil {
		log.Error("Couldn't write output json: ", err)
	} else {
		/*log.Info(bytes.NewBuffer(encodedStats).String())*/
		log.Info(string(encodedStats))
		f.Write(encodedStats)
		f.Close()
	}

	if !*sim {
		buildTraces(plan, *maxParallelConversions)
	}
}
