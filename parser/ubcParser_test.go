package parser

import "testing"
import "os"
import "encoding/hex"

import "github.com/gogo/protobuf/proto"
import "github.com/jkaiser/dedup_tools/traceProto"

func TestSkipHeader(t *testing.T) {
	Init()

	outchan := make(chan []byte)
	ubcP := NewUBCParser("ubcTesting", outchan)
	if ubcP == nil {
		t.Fatal("Couldn't initialize UBCParser")
	}

	ubcP.skipHeader()

	if line, err := ubcP.file.ReadString('\n'); err != nil {
		t.Fatalf("Next line couldn't be read: %v", err)
	} else if line[:len(line)-1] != "73e29ea83d:1" {
		t.Fatalf("wrong next line. should be 73e29ea83d:1, but got: %v", line)
	}
}

func TestReadSingleChunk(t *testing.T) {
	Init()

	outchan := make(chan []byte)
	ubcP := NewUBCParser("ubcTesting", outchan)
	if ubcP == nil {
		t.Fatal("Couldn't initialize UBCParser")
	}

	for i := 0; i < 39; i++ {
		line, _ := ubcP.file.ReadString('\n')
		t.Log(line)
	}

	chunks := ubcP.parseChunks()

	if len(chunks) != 1 {
		t.Fatalf("Wrong number of chunks. got: %v, expected: 1", len(chunks))
	}

	chunk := new(traceProto.Chunk)
	if err := proto.Unmarshal(chunks[0], chunk); err != nil {
		t.Fatalf("Couldn't unmarshal chunk: %v", err)
	} else if hex := hex.EncodeToString(chunk.GetFp()); hex != "2dc83032b5" {
		t.Fatalf("Chunk has wrong fp: got %s, expected: 2dc83032b5", chunk.GetFp())
	} else if chunk.GetCsize() != 204 {
		t.Fatalf("Chunk has wrong chunkSize: got %v, expected: 204", chunk.GetCsize())
	}
}

func TestReadManyChunks(t *testing.T) {
	// multiple chunks per file
	Init()
	outchan := make(chan []byte)

	ubcP := NewUBCParser("ubcTesting", outchan)
	if ubcP == nil {
		t.Fatal("Couldn't initialize UBCParser")
	}

	for i := 0; i < 84; i++ {
		line, _ := ubcP.file.ReadString('\n')
		t.Log(line[:len(line)-1])
	}

	chunks := ubcP.parseChunks()

	chunk := new(traceProto.Chunk)
	if err := proto.Unmarshal(chunks[0], chunk); err != nil {
		t.Fatalf("Couldn't unmarshal chunk: %v", err)
	} else if hex := hex.EncodeToString(chunk.GetFp()); hex != "dea15ab313" {
		t.Fatalf("Chunk has wrong fp: got %s, expected: dea15ab313", hex)
	} else if chunk.GetCsize() != 8109 {
		t.Fatalf("Chunk has wrong chunkSize: got %v, expected: 8109", chunk.GetCsize())
	}
	chunk = new(traceProto.Chunk)
	if err := proto.Unmarshal(chunks[len(chunks)-1], chunk); err != nil {
		t.Fatalf("Couldn't unmarshal chunk: %v", err)
	} else if hex := hex.EncodeToString(chunk.GetFp()); hex != "b8013fe5ba" {
		t.Fatalf("Chunk has wrong fp: got %s, expected: b8013fe5ba", hex)
	} else if chunk.GetCsize() != 3577 {
		t.Fatalf("Chunk has wrong chunkSize: got %v, expected: 3577", chunk.GetCsize())
	}

	if len(chunks) != 46 {
		t.Fatalf("Wrong number of chunks. got: %v, expected: 47", len(chunks))
	}
}

func TestReadFileEntry(t *testing.T) {
	Init()
	outchan := make(chan []byte, 10000)

	ubcP := NewUBCParser("ubcTesting", outchan)
	if ubcP == nil {
		t.Fatal("Couldn't initialize UBCParser")
	}

	ubcP.skipHeader()
	if !ubcP.parseFileEntry() {
		t.Fatalf("Error during parsing file")
	}

	buf := <-outchan
	file := new(traceProto.File)
	if err := proto.Unmarshal(buf, file); err != nil {
		t.Fatalf("Couldn't unmarshal file: %v", err)
	} else if string(file.GetFilename()) != "73e29ea83da7e8b0dee6c584233beb" {
		t.Fatalf("File has wrong name: got %s, expected: 73e29ea83da7e8b0dee6c584233beb", file.GetFilename())
	} else if file.GetFsize() != 0 {
		t.Fatalf("File has wrong FileSize: got %v, expected: 0", file.GetFsize())
	} else if file.GetLabel() != "c584233beb" {
		t.Fatalf("File has wrong label: got %v, expected: c584233beb", file.GetLabel())
	} else if file.GetChunkCount() != 1 {
		t.Fatalf("File wrong number of chunks: got %v, expected: 1", file.GetChunkCount())
	}

	if len(outchan) == 0 {
		t.Fatalf("parsed no chunk after fileInfo: %v", len(outchan))
	}

	buf = <-outchan
	chunk := new(traceProto.Chunk)
	if err := proto.Unmarshal(buf, chunk); err != nil {
		t.Fatalf("Couldn't unmarshal chunk: %v", err)
	} else if hex := hex.EncodeToString(chunk.GetFp()); hex != "2dc83032b5" {
		t.Fatalf("Chunk has wrong fp: got %s, expected: 2dc83032b5", hex)
	} else if chunk.GetCsize() != 204 {
		t.Fatalf("Chunk has wrong chunkSize: got %v, expected: 204", chunk.GetCsize())
	}
}

func TestReadManyFileEntries(t *testing.T) {

	Init()
	outchan := make(chan []byte, 10000)

	ubcP := NewUBCParser("ubcTesting", outchan)
	if ubcP == nil {
		t.Fatal("Couldn't initialize UBCParser")
	}

	ubcP.skipHeader()
	if !ubcP.parseFileEntry() {
		t.Fatalf("Error during parsing file")
	}

	// skip the first file and test for the second
	<-outchan
	<-outchan

	if !ubcP.parseFileEntry() {
		t.Fatalf("Error during parsing second file")
	}

	buf := <-outchan
	file := new(traceProto.File)
	if err := proto.Unmarshal(buf, file); err != nil {
		t.Fatalf("Couldn't unmarshal file: %v", err)
	} else if string(file.GetFilename()) != "73e29ea83dec58962838535740fe05" {
		t.Fatalf("File has wrong name: got %s, expected: 73e29ea83dec58962838535740fe05", file.GetFilename())
	} else if file.GetFsize() != 0 {
		t.Fatalf("File has wrong FileSize: got %v, expected: 0", file.GetFsize())
	} else if file.GetLabel() != "535740fe05" {
		t.Fatalf("File has wrong label: got %v, expected: 535740fe05", file.GetLabel())
	} else if file.GetChunkCount() != 1 {
		t.Fatalf("File wrong number of chunks: got %v, expected: 1", file.GetChunkCount())
	}

	if len(outchan) == 0 {
		t.Fatalf("parsed no chunk after fileInfo: %v", len(outchan))
	}

	buf = <-outchan
	chunk := new(traceProto.Chunk)
	if err := proto.Unmarshal(buf, chunk); err != nil {
		t.Fatalf("Couldn't unmarshal chunk: %v", err)
	} else if hex := hex.EncodeToString(chunk.GetFp()); hex != "73e29ea83d" {
		t.Fatalf("Chunk has wrong fp: got %s, expected: 73e29ea83d", hex)
	} else if chunk.GetCsize() != 144 {
		t.Fatalf("Chunk has wrong chunkSize: got %v, expected: 144", chunk.GetCsize())
	}

	if len(outchan) > 0 {
		t.Fatalf("parsed more chunks than available: %v", len(outchan))
	}
}

func TestErrorFile(t *testing.T) {

	outchan := make(chan []byte, 1e6)
	ubcP := NewUBCParser("225", outchan)
	if ubcP == nil {
		t.Fatal("Couldn't initialize UBCParser")
	}

	ubcP.ParseFile()
	//if !ubcP.parseFileEntry() {
	//t.Fatalf("Error during parsing file")
	//}

	messages := make([][]byte, 0, 10000)

	for m := range outchan {
		messages = append(messages, m)
	}

	file := new(traceProto.File)
	if err := proto.Unmarshal(messages[len(messages)-2], file); err != nil {
		t.Fatalf("Couldn't unmarshal file: %v", err)
	} else if file.GetFsize() != 129 {
		t.Fatalf("File has wrong FileSize: got %v, expected: 129", file.GetFsize())
	} else if file.GetChunkCount() != 1 {
		t.Fatalf("File wrong number of chunks: got %v, expected: 1", file.GetChunkCount())
	}

	chunk := new(traceProto.Chunk)
	if err := proto.Unmarshal(messages[len(messages)-1], chunk); err != nil {
		t.Fatalf("Couldn't unmarshal chunk: %v", err)
	} else if hex := hex.EncodeToString(chunk.GetFp()); hex != "5bbbcb15e4" {
		t.Fatalf("Chunk has wrong fp: got %s, expected: 5bbbcb15e4", hex)
	} else if chunk.GetCsize() != 333 {
		t.Fatalf("Chunk has wrong chunkSize: got %v, expected: 333", chunk.GetCsize())
	}

}

func TestMixedFileEntries(t *testing.T) {

	Init()
	outchan := make(chan []byte, 10000)

	ubcP := NewUBCParser("ubcSimpleTesting", outchan)
	if ubcP == nil {
		t.Fatal("Couldn't initialize UBCParser")
	}

	if !ubcP.parseFileEntry() {
		t.Fatalf("Error during parsing file")
	}

	buf := <-outchan
	file := new(traceProto.File)
	if err := proto.Unmarshal(buf, file); err != nil {
		t.Fatalf("Couldn't unmarshal file: %v", err)
	} else if file.GetFsize() != 5030 {
		t.Fatalf("File has wrong FileSize: got %v, expected: 5030", file.GetFsize())
	} else if file.GetLabel() != "a7459bea93" {
		t.Fatalf("File has wrong label: got %v, expected: a7459bea93", file.GetLabel())
	} else if file.GetChunkCount() != 1 {
		t.Fatalf("File wrong number of chunks: got %v, expected: 1", file.GetChunkCount())
	}
	t.Logf("read file: %v", file.String())

	if len(outchan) == 0 {
		t.Fatalf("parsed no chunk after fileInfo: %v", len(outchan))
	}

	buf = <-outchan
	chunk := new(traceProto.Chunk)
	if err := proto.Unmarshal(buf, chunk); err != nil {
		t.Fatalf("Couldn't unmarshal chunk: %v", err)
	} else if hex := hex.EncodeToString(chunk.GetFp()); hex != "45c1321490" {
		t.Fatalf("Chunk has wrong fp: got %s, expected: 45c1321490", hex)
	} else if chunk.GetCsize() != 5178 {
		t.Fatalf("Chunk has wrong chunkSize: got %v, expected: 5178", chunk.GetCsize())
	}
	t.Logf("read chunk: %v", chunk.String())

	// second file
	if !ubcP.parseFileEntry() {
		t.Fatalf("Error during parsing second file")
	}

	buf = <-outchan
	file = new(traceProto.File)
	if err := proto.Unmarshal(buf, file); err != nil {
		t.Fatalf("Couldn't unmarshal file: %v", err)
	} else if file.GetFsize() != 0 {
		t.Fatalf("File has wrong FileSize: got %v, expected: 0", file.GetFsize())
	} else if file.GetLabel() != "46f9e5725b" {
		t.Fatalf("File has wrong label: got %v, expected: 46f9e5725b", file.GetLabel())
	} else if file.GetChunkCount() != 1 {
		t.Fatalf("File wrong number of chunks: got %v, expected: 1", file.GetChunkCount())
	}
	t.Logf("read file: %v", file.String())

	if len(outchan) == 0 {
		t.Fatalf("parsed no chunk after fileInfo: %v", len(outchan))
	}

	buf = <-outchan
	chunk = new(traceProto.Chunk)
	if err := proto.Unmarshal(buf, chunk); err != nil {
		t.Fatalf("Couldn't unmarshal chunk: %v", err)
	} else if hex := hex.EncodeToString(chunk.GetFp()); hex != "ae416252a6" {
		t.Fatalf("Chunk has wrong fp: got %s, expected: ae416252a6", hex)
	} else if chunk.GetCsize() != 128 {
		t.Fatalf("Chunk has wrong chunkSize: got %v, expected: 128", chunk.GetCsize())
	}
	t.Logf("read chunk: %v", chunk.String())

	// third file
	if !ubcP.parseFileEntry() {
		t.Fatalf("Error during parsing second file")
	}
	buf = <-outchan
	file = new(traceProto.File)
	if err := proto.Unmarshal(buf, file); err != nil {
		t.Fatalf("Couldn't unmarshal file: %v", err)
	} else if file.GetFsize() != 159 {
		t.Fatalf("File has wrong FileSize: got %v, expected: 159", file.GetFsize())
	} else if file.GetLabel() != "46f9e5725b" {
		t.Fatalf("File has wrong label: got %v, expected: 46f9e5725b", file.GetLabel())
	} else if file.GetChunkCount() != 1 {
		t.Logf("read erronous file: %v", file.String())
		t.Fatalf("File 3: wrong number of chunks: got %v, expected: 1", file.GetChunkCount())
	}
	t.Logf("read file: %v", file.String())

	if len(outchan) == 0 {
		t.Fatalf("parsed no chunk after fileInfo: %v", len(outchan))
	}

	buf = <-outchan
	chunk = new(traceProto.Chunk)
	if err := proto.Unmarshal(buf, chunk); err != nil {
		t.Fatalf("Couldn't unmarshal chunk: %v", err)
	} else if hex := hex.EncodeToString(chunk.GetFp()); hex != "9aa619dcc3" {
		t.Fatalf("Chunk has wrong fp: got %s, expected: 9aa619dcc3", hex)
	} else if chunk.GetCsize() != 307 {
		t.Fatalf("Chunk has wrong chunkSize: got %v, expected: 307", chunk.GetCsize())
	}
	t.Logf("read chunk: %v", chunk.String())

	if len(outchan) > 0 {
		t.Fatalf("parsed more chunks than available: %v", len(outchan))
	}
}

func TestParseFile(t *testing.T) {
	Init()
	outchan := make(chan []byte, 10000)

	ubcP := NewUBCParser("ubcTesting", outchan)
	if ubcP == nil {
		t.Fatal("Couldn't initialize UBCParser")
	}

	ubcP.ParseFile()

	buffers := make([][]byte, 0)
	for b := range outchan {
		buffers = append(buffers, b)
	}

	if len(buffers) != 53 {
		t.Fatalf("Wrong number of protobufs: got %v, expected: 53", len(buffers))
	}
	t.Log("num buffers: ", len(buffers))

	chunk := new(traceProto.Chunk)
	if err := proto.Unmarshal(buffers[len(buffers)-1], chunk); err != nil {
		t.Fatalf("Couldn't unmarshal chunk: %v", err)
	} else if hex := hex.EncodeToString(chunk.GetFp()); hex != "b8013fe5ba" {
		t.Fatalf("Chunk has wrong fp: got %s, expected: b8013fe5ba", hex)
	} else if chunk.GetCsize() != 3577 {
		t.Fatalf("Chunk has wrong chunkSize: got %v, expected: 3577", chunk.GetCsize())
	}

}

func Init() {
	testText := `Backup Stream
00000000005c
00000000005c
C:\\Windows\\system32
1252126932
Microsoft Windows Vista Enterprise Edition, 32-bit Service Pack 2 (build 6002)
Arch:0
Level:6
Rev:3851
Procs:2
Phys:3486244864
Page:7197949952
Virt:2147352576
VolumeName:<Not Collected>
SerialNumber:0x6487fc99
MaxComponentLength:255
Flags:0x2f00ff
Filesystem:NTFS
SectorsPerCluster:8
BytesPerSector:512
NumberOfFreeClusters:29403763
TotalNumberOfClusters:61014527
TotalReserved:0
BytesPerFileRecordSegment:1024
Mft2StartLcn:30507263
MftStartLcn:786432
MftValidDataLength:590217216
MftZoneEnd:15804672
MftZoneStart:15753440
FSCreation:128508610614192264
NTFSByteCount:8
NTFSMajorVersion:3
NTFSMinorVersion:1

73e29ea83d:1
a7e8b0dee6:8
c584233beb:0
0
0
16
8444249301483345
1
0
128069398392366250
128932114418389156
128932114418389156
SV:0
2dc83032b5:204

73e29ea83d:1
ec58962838:32
535740fe05:0
0
0
10
71776119061651690
1
0
128951011107594693
128951012473981318
128951012473981318
SV:0
V:2:L:1147380
73e29ea83d:144

73e29ea83d:1
e2fb615859:8
47ca676f42:0
0
355
27
844424930309165
1
0
128444944970468750
128468453783593750
128469067702968750
fa1f66e769:535

73e29ea83d:1
2b7eb4f940:7
535740fe05:0
0
333257
27
1970324836983539
1
0
128469067691406250
128916447956830916
128839053980000000
SV:0
V:82:L:33359015
dea15ab313:8109
82c98ad558:5471
84985fddbf:4096
d625707c12:4096
d6352dca8b:4096
3b71c7ecb3:4096
3243a6db31:6536
51c0152385:5976
b12ca2f4b2:6848
d3a3a0029a:6652
31df357701:4128
83bbaee9aa:9731
cb1f5cc285:9323
db74bd40c8:8824
46f45dd8d1:4246
fea3f1fe8a:8609
f0bf87132d:10343
f3f08c2815:12841
959d5f0fc4:4481
f64fcf7e94:8000
f5dbe9b100:4717
73edd52dfd:5523
8acaa12a06:7779
3154f0caa5:6913
5d83fc1bb7:6493
b40a43b575:7723
6141da39b1:6371
77fd5674c1:5906
a91346082a:11667
9548bede38:4698
77ce889d76:6053
187c770d00:11286
60468c0ea5:19877
91dce24888:15846
9e46efe81e:4104
91c555f3c5:4398
985f86a2c9:6905
c937a2ff80:4642
03776ef329:6060
394915ecdf:8133
cc0e764bbb:4989
d26784062f:7857
19beba1bf8:11710
a5ca738b29:6201
745f7e31da:7567
b8013fe5ba:3577

`

	if _, err := os.Stat("ubcTesting"); err == nil {
		os.Remove("ubcTesting")
	}
	f, _ := os.OpenFile("ubcTesting", os.O_WRONLY|os.O_CREATE, 0666)
	f.Write([]byte(testText))
	f.Close()

	simpleFileTest := `7d7488baa2:24
4c70343077:10
a7459bea93:0
1
5030
20
27584547717732928
1
0
128951164845353982
128951164845353982
128951165113473982
SV:0
V:2:L:706408
45c1321490:5178

7d7488baa2:24
e270ae5061:7
46f9e5725b:0
1
0
20
27584547717733106
1
0
128951165115173982
128951165115173982
128951167777393982
ae416252a6:128

7d7488baa2:24
479c997a92:3
46f9e5725b:0
1
159
20
23362423067073467
1
0
128951165474953982
128951165474953982
128951167682213982
9aa619dcc3:307

`

	if _, err := os.Stat("ubcSimpleTesting"); err == nil {
		os.Remove("ubcSimpleTesting")
	}
	f, _ = os.OpenFile("ubcSimpleTesting", os.O_WRONLY|os.O_CREATE, 0666)
	f.Write([]byte(simpleFileTest))
	f.Close()

}
