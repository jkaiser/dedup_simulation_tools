package main

import "testing"
import "os"
import "github.com/gogo/protobuf/proto"
import "github.com/jkaiser/dedup_simulations/de_pc2_dedup_fschunk"

func protoParseTestInit(t *testing.T) map[string]string {
	// create first testdata

	f := new(de_pc2_dedup_fschunk.File)
	f.Filename = proto.String("filename A.dmtcp")
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
	f.ChunkCount = proto.Uint32(5)
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
		c := new(de_pc2_dedup_fschunk.Chunk)
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

	// the last chunk appears two times:
	c := new(de_pc2_dedup_fschunk.Chunk)
	fp := make([]byte, 10)
	fp[0] = byte(0)
	c.Fp = fp
	c.Csize = proto.Uint32(uint32(0))

	buf, err = proto.Marshal(c)
	if err != nil {
		t.Fatalf("Couldn't marshal second test data: %v", err)
		return nil
	}

	testfile.Write(proto.EncodeVarint(uint64(len(buf))))
	testfile.Write(buf)

	testdata["FileWith5Chunks"] = "protoTestingFilledFile"
	testfile.Close()
	return testdata
}

func TestComputeSkewEmptyFile(t *testing.T) {
	testdata := protoParseTestInit(t)

	refs, _ := computeSkewness([]string{testdata["emptyFile"]})
	if len(refs) > 1 {
		t.Fatalf("Empty file returned too big refcnt list: expected: 1 entrie, got : %v entries: %v", len(refs), refs)
	}
}

func TestComputeSkew5Chunks(t *testing.T) {
	testdata := protoParseTestInit(t)

	refs, _ := computeSkewness([]string{testdata["FileWith5Chunks"]})
	if len(refs) != 3 {
		t.Fatalf("Wrong length of refcnt list. expected: %v; got: %v entries %v", 3, len(refs), refs)
	} else if refs[1] != 3 {
		t.Fatalf("Wrong refcount for 1 occurence. expected: %v; got: %v", 3, refs[1])
	} else if refs[2] != 1 {
		t.Fatalf("Wrong refcount for 2 occurences. expected: %v; got: %v", 1, refs[2])
	}
}
