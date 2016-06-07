package parser

import "os"
import "bufio"
import "io"
import "strings"
import "strconv"
import "regexp"
import "encoding/hex"

import "github.com/gogo/protobuf/proto"
import "github.com/jkaiser/dedup_tools/traceProto"

import log "github.com/cihub/seelog"

type UBCParser struct {
	filename      string
	file          *bufio.Reader
	outputChan    chan<- []byte
	colonSeperate *regexp.Regexp
}

func NewUBCParser(filepath string, outChan chan<- []byte) *UBCParser {
	parser := new(UBCParser)
	parser.filename = filepath
	parser.outputChan = outChan
	if re, err := regexp.Compile("([0-9a-fz]+):([0-9]+)"); err != nil {
		log.Error(err)
		return nil
	} else {
		parser.colonSeperate = re
	}

	if f, err := os.Open(filepath); err != nil {
		log.Error("Couldn't open file to parse: ", err)
		return nil
	} else {
		parser.file = bufio.NewReaderSize(f, 4*1024*1024)
		return parser
	}
}

func (p *UBCParser) skipHeader() bool {
	line, err := p.file.ReadString('\n')
	if err != nil {
		log.Error("Error while parsing header: ", err)
		panic(err)
	}

	for (err == nil) && len(line) > 0 {
		line, err = p.file.ReadString('\n')
		line = strings.TrimSpace(line)
	}

	if err != nil {
		log.Error("Error while parsing header: ", err)
		panic(err)
	}

	return true
}

// Parses the whole file
func (p *UBCParser) ParseFile() {

	p.skipHeader()
	for p.parseFileEntry() {
	}
	close(p.outputChan)
}

func (p *UBCParser) parseChunks() [][]byte {
	chunks := make([][]byte, 0, 100)

	var err error
	var line string

	// skip rest of the 7 file metainfo lines
	for i := 0; i < 7; i++ {
		line, err = p.file.ReadString('\n')
		if err != nil {
			log.Error("Couldn't parse line: ", err)
			return nil
		}
	}

	// skip file frag information
	for {
		line, err = p.file.ReadString('\n')
		if err != nil {
			log.Error("Couldn't read frag info line: ", err)
			return nil
		}

		line = strings.TrimSpace(line)
		if !(strings.HasPrefix(line, "SV:") || strings.HasPrefix(line, "V:") || strings.HasPrefix(line, "A:")) {
			break
		}
	}

	// read chunks
	protoChunk := new(traceProto.Chunk)
	for len(line) > 0 {
		lineParts := strings.Split(line, ":")
		if len(lineParts) != 2 {
			log.Error("Found strange line while parsing chunk: \n", line, ", parts: ", lineParts)
		}

		if lineParts[0] == "zzzzzzzzzzzz" {
			protoChunk.Fp, _ = hex.DecodeString("000000000000")
		} else if fp_array, err := hex.DecodeString(lineParts[0]); err == nil {
			protoChunk.Fp = fp_array
		} else {
			log.Errorf("Couldn't decode fp %v : %v", lineParts[0], err)
		}
		if sz, err := strconv.ParseUint(lineParts[1], 10, 32); err != nil {
			log.Error("Could not parse chunk size (", lineParts[2], ") in line: \n", line)
			panic(err)
		} else {
			size := uint32(sz)
			protoChunk.Csize = &size
		}

		if newbuf, err := protoChunk.Marshal(); err != nil {
			log.Error("Couldn't marshal protobuf chunk message: ", err)
			panic(err)
		} else {
			chunks = append(chunks, newbuf)
		}

		// cleanup and read next line
		protoChunk.Reset()
		line, err = p.file.ReadString('\n')
		if err != nil {
			log.Error("Couldn't parse chunk line")
			return nil
		}
		line = strings.TrimSpace(line)
	}

	return chunks
}

func (p *UBCParser) parseFileEntry() bool {

	// parse filename out of dirhash etc

	var line string
	var err error
	// parse dirname
	if line, err = p.file.ReadString('\n'); err == io.EOF {
		return false
	} else if err != nil {
		log.Error("Error while parsing file dirname: ", err)
		panic(err)
	} else if strings.Contains(line, "LOGCOMPLETE") {
		return false
	}
	line = strings.TrimSpace(line)
	dirInfo := p.colonSeperate.FindStringSubmatch(line)

	// parse filename
	if line, err = p.file.ReadString('\n'); err != nil {
		log.Error("Error while parsing filename : ", err, "\n previous line was: ", dirInfo[0])
		panic(err)
	}
	line = strings.TrimSpace(line)
	fileInfo := p.colonSeperate.FindStringSubmatch(line)

	// parse extention
	if line, err = p.file.ReadString('\n'); err != nil {
		log.Error("Error while parsing fileextension: ", err, "\n previous line was: ", fileInfo[0])
		panic(err)
	}
	line = strings.TrimSpace(line)
	extensionInfo := p.colonSeperate.FindStringSubmatch(line)

	f := new(traceProto.File)

	name := dirInfo[1] + fileInfo[1] + extensionInfo[1]
	f.Filename = proto.String(name)
	label := extensionInfo[1]
	f.Label = proto.String(label)

	// parse file size
	p.file.ReadString('\n')
	if line, err = p.file.ReadString('\n'); err != nil {
		log.Error("Error while parsing file size", err)
		panic(err)
	}
	line = strings.TrimSpace(line)
	if size, err := strconv.ParseUint(line, 10, 64); err != nil {
		log.Error("Error while regex-parsing file size: ", err)
		panic(err)
	} else {
		f.Fsize = proto.Uint64(size)
	}

	chunks := p.parseChunks()
	var numChunks uint32 = uint32(len(chunks))
	f.ChunkCount = proto.Uint32(numChunks)

	// marshal and send fileobj
	if newbuf, err := f.Marshal(); err != nil {
		log.Error("Error while marshalling file: ", err)
		panic(err)
	} else {
		p.outputChan <- newbuf
	}

	for _, chunk := range chunks {
		p.outputChan <- chunk
	}

	return true
}
