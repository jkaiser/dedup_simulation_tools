package parser

import "os"
import "bufio"
import "io"
import "strings"
import "strconv"

import "github.com/jkaiser/dedup_tools/traceProto"

import proto "code.google.com/p/goprotobuf/proto"
import log "github.com/cihub/seelog"

type LegacyParser struct {
	filename   string
	file       *bufio.Reader
	outputChan chan<- []byte

	chunks    []*traceProto.Chunk
	msgBuffer *proto.Buffer
}

func NewLegacyParser(filepath string, outChan chan<- []byte) *LegacyParser {
	parser := new(LegacyParser)
	parser.filename = filepath
	parser.outputChan = outChan
	parser.msgBuffer = proto.NewBuffer(nil)

	if f, err := os.Open(filepath); err != nil {
		log.Error("Couldn't open file to parse: ", err)
		return nil
	} else {
		parser.file = bufio.NewReaderSize(f, 4*1024*1024)
		return parser
	}
}

func (p *LegacyParser) helperChunkSize(buffer []byte, offset int) int64 {

	result := int64(0)
	i := int64(buffer[offset+3])
	result = 256*result + i
	i = int64(buffer[offset+2])
	result = 256*result + i
	i = int64(buffer[offset+1])
	result = 256*result + i
	i = int64(buffer[offset+0])
	result = 256*result + i

	return result
}

// Parses the whole file
func (p *LegacyParser) ParseFile() {
	filecount := 0
	for p.parseFileEntry() {
		filecount++
	}
	log.Info("Filecount in ParseFile() after finish ", filecount)
	close(p.outputChan)
}

func (p *LegacyParser) parseFileEntry() bool {
	f := new(traceProto.File)

	line, err := p.file.ReadString('\n')
	if err != nil {
		if err == io.EOF {
			return false
		} else {
			log.Error("parseFileEntry() p.ReadLine() error: ", err)
			return false
		}
	}
	line = strings.Trim(line, "\n")
	elements := strings.Split(line, "\t")

	if len(elements) != 2 && len(elements) != 3 {
		log.Error("wrong number of elements: ", len(elements))
		return false
	}

	filename := elements[0]
	filesize, _ := strconv.ParseInt(elements[1], 10, 64)

	var filetype string
	if len(elements) >= 3 {
		filetype = elements[2]
	} else {
		filetype = ""
	}
	f.Filename = proto.String(filename)
	f.Fsize = proto.Uint64(uint64(filesize))
	f.Type = proto.String(filetype)

	chunkcount := 0
	rsf := false
	for ok, _ := p.parseChunks(); ok; ok, rsf = p.parseChunks() {
		chunkcount++
	}

	if rsf {
		log.Infof("Filename: %s, chunkcount: %d, filetype: %s", filename, chunkcount, filetype)

	}

	f.ChunkCount = proto.Uint32(uint32(chunkcount))
	msg, _ := proto.Marshal(f)
	p.outputChan <- msg
	for _, c := range p.chunks {
		cmsg, _ := proto.Marshal(c)
		p.outputChan <- cmsg

	}
	p.chunks = nil
	return true

}

func (p *LegacyParser) parseChunks() (bool, bool) {
	chunk := new(traceProto.Chunk)
	rS, size, _ := p.file.ReadRune()
	_ = size
	recordSize := int32(rS)
	if recordSize == -1 {
		log.Info("Finished chunks. recordsize: ", recordSize)
		return false, false
	}

	if recordSize == 0 {

		p.file.ReadString('\n')

		return false, false
	}

	if recordSize != 24 {

		log.Error("recordsize in parseChunks() != 24. recordsize == ", recordSize)

		return false, true
	}
	buffer := make([]byte, 4)
	k, err := p.file.Read(buffer)
	if err != nil {
		log.Error("Read(buffer) in parseChunks: ", err)
	}
	if k < 4 {
		for dif := 4 - k; dif > 0; dif = 4 - k {
			m, _ := p.file.Read(buffer[k:])
			k = k + m
		}

	}
	chunksize := p.helperChunkSize(buffer, 0)
	if chunksize >= 64*1024 || chunksize < 0 {
		log.Error("Illegal chunksize: ", chunksize)
		return false, false
	} else {
		fp := make([]byte, 20)
		n, err := p.file.Read(fp)
		if err != nil {
			log.Error("Read(buffer) in parseChunks: ", err)
		}
		if n < 20 {
			for dif := 20 - n; dif > 0; dif = 20 - n {
				m, _ := p.file.Read(fp[n:])
				n = n + m
			}
		}
		chunk.Fp = make([]byte, 20)
		copy(chunk.Fp, fp)
	}
	chunk.Csize = proto.Uint32(uint32(chunksize))
	p.chunks = append(p.chunks, chunk)

	return true, false
}

func (p *LegacyParser) parseNChunks(n uint32) bool {

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

func (p *LegacyParser) readNextVarint() (uint64, error) {

	buf := make([]byte, 0, 5)
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
