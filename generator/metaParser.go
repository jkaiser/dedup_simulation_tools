package main

import "time"
import "io/ioutil"
import "encoding/json"
import log "github.com/cihub/seelog"

/*
{"CurrentTime": 1253343782,
"TotalNumberOfClusters": 5172479,
"Hostname": "0000000001ec",
"Rev": 1027,
"NTFSByteCount": 8,
"BytesPerFileRecordSegment": 1024,
"Mft2StartLcn": 2586239,
"Username": "0000000001b2",
"SystemDirectory": "C:\\Windows\\system32",
"Phys":         3756146688,
"VolumeName": "<Not Collected>",
"BytesPerSector": 512,
"StreamType": "Backup Stream",
"Arch": 0,
"MaxComponentLength": 255,
"Procs": 1, "SerialNumber":
"0xc809853f",
"Level": 15,
"traceFile": "1633.gz",
"SectorsPerCluster": 8,
"traceRun": "64fb",
"MftValidDataLength": 1310720,
"Flags": "0x1ef00ff", "MftZoneEnd": 837952, "Filesystem": "NTFS", "Page": 7510351872, "Virt": 2147352576, "NumberOfFreeClusters": 4656043, "FSCreation": 128478328970671267, "TotalReserved": 0, "MftZoneStart": 786752, "MftStartLcn":          786432, "OS": "Microsoft Windows 7  (build 7271)"}
*/

type MSTraceFile struct {
	CurrentTime int64
	Hostname    string
	Username    string
	TraceFile   string
	TraceRun    string

	time      time.Time
	diffToMin time.Duration
}

func loadMetadata(path string, traceRun string) []*MSTraceFile {
	log.Info("Read metadata...")

	var buf []byte
	var err error
	if buf, err = ioutil.ReadFile(path); err != nil {
		log.Error("Couldn't read metadata file: ", err)
		return nil
	}

	allFiles := make([]MSTraceFile, 0, 1000)
	if err := json.Unmarshal(buf, &allFiles); err != nil {
		log.Error("Couldn't unmarshal metadata file: ", err)
	}

	wishedTraceFiles := make([]*MSTraceFile, 0, 1000)
	for i := range allFiles {
		if allFiles[i].TraceRun == traceRun {
			wishedTraceFiles = append(wishedTraceFiles, &allFiles[i])
		}
	}

	log.Debug("will return ", len(wishedTraceFiles), " infos for trace ", traceRun)
	return wishedTraceFiles
}

func addRelativeTime(traces []*MSTraceFile) {
	log.Info("Add relative times...")

	var minTime int64 = traces[0].CurrentTime
	for i := range traces {
		if traces[i].CurrentTime < minTime {
			minTime = traces[i].CurrentTime
		}

		traces[i].time = time.Unix(traces[i].CurrentTime, 0)
	}

	min := time.Unix(minTime, 0)
	for i := range traces {
		traces[i].diffToMin = traces[i].time.Sub(min)
	}
}
