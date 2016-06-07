package parser

import "os"
import "bufio"
import "io"
import "github.com/jkaiser/dedup_tools/traceProto"

import "github.com/gogo/protobuf/proto"
import log "github.com/cihub/seelog"

type ProtoParser struct {
	filename   string
	file       *bufio.Reader
	rawFile    *os.File
	outputChan chan<- []byte

	msgBuffer *proto.Buffer
}

func NewProtoParser(filepath string, outChan chan<- []byte) *ProtoParser {
	parser := new(ProtoParser)
	parser.filename = filepath
	parser.outputChan = outChan
	parser.msgBuffer = proto.NewBuffer(nil)

	if f, err := os.Open(filepath); err != nil {
		log.Error("Couldn't open file to parse: ", err)
		return nil
	} else {
		parser.file = bufio.NewReaderSize(f, 4*1024*1024)
		parser.rawFile = f
		return parser
	}
}

// Parses the whole file
func (p *ProtoParser) ParseFile() {
	filecount := 0
	for p.parseFileEntry() {
		filecount++
	}
	//log.Info("Filecount in ParseFile() after finish ", filecount)
	close(p.outputChan)
}

func (p *ProtoParser) parseFileEntry() bool {

	var msgSize uint64
	var err error

	// varint
	if msgSize, err = p.readNextVarint(); err != nil {
		return false
	}
	//log.Info("MsgSize: the first VarInt: ", msgSize)
	buf := make([]byte, msgSize)
	if n, err := io.ReadFull(p.file, buf); err != nil {
		pos, _ := p.rawFile.Seek(0, 1)
		log.Errorf("Could not read next FileMsg of size %v, only read %v bytes. pos: %v, err: %v", msgSize, n, pos, err)
		return false
	}

	p.outputChan <- buf

	f := new(traceProto.File)
	if err := proto.Unmarshal(buf, f); err != nil {
		log.Errorf("ProtoParser: Could not unmarshal next FileMsg of size %v: %v", len(buf), err)
		return false
	}

	if !p.parseNChunks(f.GetChunkCount()) {
		log.Error("ProtoParser: Error during parsing chunks")
		return false
	}
	return true
}

func (p *ProtoParser) parseNChunks(n uint32) bool {

	var msgSize uint64
	var err error

	// read chunks
	for i := uint32(0); i < n; i++ {

		// varint
		if msgSize, err = p.readNextVarint(); err != nil {
			log.Error("Couldn't read next chunkMsg size: ", err)
			return false
		}

		// msg
		buf := make([]byte, msgSize)
		if _, err := io.ReadFull(p.file, buf); err != nil {
			log.Error("Could not read next ChunkMsg: ", err)
			return false
		}

		p.outputChan <- buf
	}

	return true
}

func (p *ProtoParser) readNextVarint() (uint64, error) {

	buf := make([]byte, 0, 10)
	for {
		if b, err := p.file.ReadByte(); err != nil {
			return 0, err
		} else {
			buf = append(buf, b)
			n, consumed := proto.DecodeVarint(buf)
			if (n != 0) || (consumed != 0) {
				return n, err
			}
		}
	}
}
