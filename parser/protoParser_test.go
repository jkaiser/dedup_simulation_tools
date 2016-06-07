package parser

import "testing"

import "os"
import "io/ioutil"

//import "code.google.com/p/goprotobuf/proto"
import "github.com/gogo/protobuf/proto"
import "github.com/jkaiser/dedup_tools/traceProto"

func protoParseTestInit(t *testing.T) map[string]string {
	// create first testdata

	f := new(traceProto.File)
	f.Filename = proto.String("filename A")
	f.Fsize = proto.Uint64(42)
	f.Label = proto.String("label A")
	f.Type = proto.String("type A")
	f.ChunkCount = proto.Uint32(0)

	buf, err := proto.Marshal(f)
	if err != nil {
		t.Fatalf("Couldn't marshal test data: %v", err)
		return nil
	}

	// create file
	if _, err := os.Stat("protoTestingEmpty"); err == nil {
		os.Remove("protoTestingEmpty")
	}
	testfile, _ := os.OpenFile("protoTestingEmpty", os.O_WRONLY|os.O_CREATE, 0666)
	testfile.Write(proto.EncodeVarint(uint64(len(buf))))
	testfile.Write(buf)
	testfile.Close()

	testdata := make(map[string]string)
	testdata["emptyFile"] = "protoTestingEmpty"

	// second file
	f.ChunkCount = proto.Uint32(4)
	buf, err = proto.Marshal(f)
	if err != nil {
		t.Fatalf("Couldn't marshal second test data: %v", err)
		return nil
	}

	if _, err := os.Stat("protoTestingFilledFile"); err == nil {
		os.Remove("protoTestingFilledFile")
	}
	testfile, _ = os.OpenFile("protoTestingFilledFile", os.O_WRONLY|os.O_CREATE, 0666)
	testfile.Write(proto.EncodeVarint(uint64(len(buf))))
	testfile.Write(buf)

	for i := 0; i < 4; i++ {
		c := new(traceProto.Chunk)
		fp := make([]byte, 10)
		fp[0] = byte(i)
		c.Fp = fp
		c.Csize = proto.Uint32(uint32(i))

		buf, err = proto.Marshal(c)
		if err != nil {
			t.Fatalf("Couldn't marshal second test data: %v", err)
			return nil
		}

		testfile.Write(proto.EncodeVarint(uint64(len(buf))))
		testfile.Write(buf)
	}

	testdata["FileWith4Chunks"] = "protoTestingFilledFile"
	testfile.Close()

	if _, err := os.Stat("protoTesting4Chunks"); err == nil {
		os.Remove("protoTesting4Chunks")
	}
	testfile, _ = os.OpenFile("protoTesting4Chunks", os.O_WRONLY|os.O_CREATE, 0666)

	// 4 chunks
	totalLen := 0
	for i := 0; i < 4; i++ {
		c := new(traceProto.Chunk)
		fp := make([]byte, 10)
		fp[0] = byte(i)
		c.Fp = fp
		c.Csize = proto.Uint32(uint32(i))

		buf, err = proto.Marshal(c)
		if err != nil {
			t.Fatalf("Couldn't marshal second test data: %v", err)
			return nil
		}
		t.Logf("chunk %v: len = %v, buf=%x", i, len(buf), buf)
		totalLen += len(buf)
		varintBuf := proto.EncodeVarint(uint64(len(buf)))
		totalLen += len(varintBuf)
		testfile.Write(varintBuf)
		testfile.Write(buf)
	}

	t.Logf("total Len 4chunks : %v", totalLen)
	testfile.Close()

	testdata["4chunks"] = "protoTesting4Chunks"
	return testdata
}

func TestParseEmptyFile(t *testing.T) {
	testdata := protoParseTestInit(t)
	messageChan := make(chan []byte, 1000)
	protoParser := NewProtoParser(testdata["emptyFile"], messageChan)

	if !protoParser.parseFileEntry() {
		t.Fatal("Error during empty file parsing.")
	}

	buf, ok := <-messageChan
	if !ok {
		t.Fatal("No message was parsed")
	}

	f := new(traceProto.File)
	if err := proto.Unmarshal(buf, f); err != nil {
		t.Fatalf("Couldn't unmarshal test protobuf: %v", err)
	}

	if f.GetFilename() != "filename A" {
		t.Fatalf("Wrong filename: got %v", f.GetFilename())
	} else if f.GetFsize() != 42 {
		t.Fatalf("Wrong siae: got %v", f.GetFsize())
	} else if f.GetLabel() != "label A" {
		t.Fatalf("Wrong label: got %v", f.GetLabel())
	} else if f.GetType() != "type A" {
		t.Fatalf("Wrong type: got %v", f.GetType())
	} else if f.GetChunkCount() != 0 {
		t.Fatalf("Wrong chunkCount: got %v", f.GetChunkCount())
	}

	// should be finished now
	if protoParser.parseFileEntry() {
		t.Fatal("parseFileEntry returned true while EOF.")
	}

	// now on 'ParseFile'
	messageChan = make(chan []byte, 1000)
	protoParser = NewProtoParser(testdata["emptyFile"], messageChan)

	protoParser.ParseFile()

	buf, ok = <-messageChan
	if !ok {
		t.Fatal("No message was parsed")
	}

	f = new(traceProto.File)
	if err := proto.Unmarshal(buf, f); err != nil {
		t.Fatalf("Couldn't unmarshal test protobuf: %v", err)
	}

	if f.GetFilename() != "filename A" {
		t.Fatalf("Wrong filename: got %v", f.GetFilename())
	} else if f.GetFsize() != 42 {
		t.Fatalf("Wrong siae: got %v", f.GetFsize())
	} else if f.GetLabel() != "label A" {
		t.Fatalf("Wrong label: got %v", f.GetLabel())
	} else if f.GetType() != "type A" {
		t.Fatalf("Wrong type: got %v", f.GetType())
	} else if f.GetChunkCount() != 0 {
		t.Fatalf("Wrong chunkCount: got %v", f.GetChunkCount())
	}
}

// Tests input file with 2x the same fileentry
func TestParseFilledFile2(t *testing.T) {
	testdata := protoParseTestInit(t)
	buf, err := ioutil.ReadFile(testdata["FileWith4Chunks"])
	if err != nil {
		t.Fatalf("input file not available")
	}
	if _, err := os.Stat("protoTesting2Files"); err == nil {
		os.Remove("protoTesting2Files")
	}
	testfile, _ := os.OpenFile("protoTesting2Files", os.O_WRONLY|os.O_CREATE, 0666)
	testfile.Write(buf)
	testfile.Write(buf)
	testfile.Close()

	messageChan := make(chan []byte, 1000)
	protoParser := NewProtoParser("protoTesting2Files", messageChan)

	// now on 'ParseFile'
	protoParser.ParseFile()

	buf, ok := <-messageChan
	if !ok {
		t.Fatal("No message was parsed")
	}

	f := new(traceProto.File)
	if err := proto.Unmarshal(buf, f); err != nil {
		t.Fatalf("Couldn't unmarshal test protobuf: %v", err)
	}

	if f.GetFilename() != "filename A" {
		t.Fatalf("Wrong filename: got %v", f.GetFilename())
	} else if f.GetFsize() != 42 {
		t.Fatalf("Wrong siae: got %v", f.GetFsize())
	} else if f.GetLabel() != "label A" {
		t.Fatalf("Wrong label: got %v", f.GetLabel())
	} else if f.GetType() != "type A" {
		t.Fatalf("Wrong type: got %v", f.GetType())
	} else if f.GetChunkCount() != 4 {
		t.Fatalf("Wrong chunkCount: got %v", f.GetChunkCount())
	}

	for i := 0; i < 4; i++ {
		chunk := new(traceProto.Chunk)
		if buf, ok := <-messageChan; !ok {
			t.Fatal("No message was parsed")
		} else if err := proto.Unmarshal(buf, chunk); err != nil {
			t.Fatalf("Couldn't unmarshal test protobuf: %v", err)
		}

		if chunk.GetFp()[0] != byte(i) {
			t.Fatalf("Chunk %v has wrong fp, got: %x", i, chunk.GetFp())
		} else if chunk.GetCsize() != uint32(i) {
			t.Fatalf("Chunk %v has wrong size, got: %v", i, chunk.GetCsize())
		}
	}

	// second round(file)

	buf, ok = <-messageChan
	if !ok {
		t.Fatal("No message was parsed")
	}

	f = new(traceProto.File)
	if err := proto.Unmarshal(buf, f); err != nil {
		t.Fatalf("Couldn't unmarshal test protobuf: %v", err)
	}

	if f.GetFilename() != "filename A" {
		t.Fatalf("Wrong filename: got %v", f.GetFilename())
	} else if f.GetFsize() != 42 {
		t.Fatalf("Wrong siae: got %v", f.GetFsize())
	} else if f.GetLabel() != "label A" {
		t.Fatalf("Wrong label: got %v", f.GetLabel())
	} else if f.GetType() != "type A" {
		t.Fatalf("Wrong type: got %v", f.GetType())
	} else if f.GetChunkCount() != 4 {
		t.Fatalf("Wrong chunkCount: got %v", f.GetChunkCount())
	}

	for i := 0; i < 4; i++ {
		chunk := new(traceProto.Chunk)
		if buf, ok := <-messageChan; !ok {
			t.Fatal("No message was parsed")
		} else if err := proto.Unmarshal(buf, chunk); err != nil {
			t.Fatalf("Couldn't unmarshal test protobuf: %v", err)
		}

		if chunk.GetFp()[0] != byte(i) {
			t.Fatalf("Chunk %v has wrong fp, got: %x", i, chunk.GetFp())
		} else if chunk.GetCsize() != uint32(i) {
			t.Fatalf("Chunk %v has wrong size, got: %v", i, chunk.GetCsize())
		}
	}
}

func TestParseFilledFile(t *testing.T) {
	testdata := protoParseTestInit(t)

	messageChan := make(chan []byte, 1000)
	protoParser := NewProtoParser(testdata["FileWith4Chunks"], messageChan)

	// now on 'ParseFile'
	protoParser.ParseFile()

	buf, ok := <-messageChan
	if !ok {
		t.Fatal("No message was parsed")
	}

	f := new(traceProto.File)
	if err := proto.Unmarshal(buf, f); err != nil {
		t.Fatalf("Couldn't unmarshal test protobuf: %v", err)
	}

	if f.GetFilename() != "filename A" {
		t.Fatalf("Wrong filename: got %v", f.GetFilename())
	} else if f.GetFsize() != 42 {
		t.Fatalf("Wrong siae: got %v", f.GetFsize())
	} else if f.GetLabel() != "label A" {
		t.Fatalf("Wrong label: got %v", f.GetLabel())
	} else if f.GetType() != "type A" {
		t.Fatalf("Wrong type: got %v", f.GetType())
	} else if f.GetChunkCount() != 4 {
		t.Fatalf("Wrong chunkCount: got %v", f.GetChunkCount())
	}

	for i := 0; i < 4; i++ {
		chunk := new(traceProto.Chunk)
		if buf, ok := <-messageChan; !ok {
			t.Fatal("No message was parsed")
		} else if err := proto.Unmarshal(buf, chunk); err != nil {
			t.Fatalf("Couldn't unmarshal test protobuf: %v", err)
		}

		if chunk.GetFp()[0] != byte(i) {
			t.Fatalf("Chunk %v has wrong fp, got: %x", i, chunk.GetFp())
		} else if chunk.GetCsize() != uint32(i) {
			t.Fatalf("Chunk %v has wrong size, got: %v", i, chunk.GetCsize())
		}
	}
}

func TestParseChunks(t *testing.T) {
	testdata := protoParseTestInit(t)

	// 4 out of 4 chunks
	messageChan := make(chan []byte, 1000)
	protoParser := NewProtoParser(testdata["4chunks"], messageChan)

	if !protoParser.parseNChunks(4) {
		t.Fatal("Error while parsing 4 chunks")
	}

	for i := 0; i < 4; i++ {
		chunk := new(traceProto.Chunk)
		if buf, ok := <-messageChan; !ok {
			t.Fatal("No message was parsed")
		} else if err := proto.Unmarshal(buf, chunk); err != nil {
			t.Fatalf("Couldn't unmarshal test protobuf: %v, buflen: %v, buf is: %x", err, len(buf), buf)
		}

		if chunk.GetFp()[0] != byte(i) {
			t.Fatalf("Chunk %v has wrong fp, got: %x", i, chunk.GetFp())
		} else if chunk.GetCsize() != uint32(i) {
			t.Fatalf("Chunk %v has wrong size, got: %v", i, chunk.GetCsize())
		}
	}
}
