package gs

import (
	"io"
	"log"
	"os"
	"sync"

	"golang.org/x/net/context"
)

func DownloadFile(gsFile *File, osFile *os.File, workerCount int64) error {
	size, err := gsFile.Size()
	if err != nil {
		return err
	}

	log.Printf("Writing to temp file %s", osFile.Name())

	// New worker struct to download file
	var worker = Worker{
		File:       osFile,
		RemoteFile: gsFile,
		Count:      workerCount,
		TotalSize:  int64(size),
		Chunk:      make(chan chunk),
	}

	var start, end int64
	var partial_size = int64(size) / workerCount

	// Start file writer
	killSig := make(chan bool)
	go worker.writeFile(killSig)

	for num := int64(0); num < worker.Count; num++ {
		if num == worker.Count {
			end = int64(size)
		} else {
			end = start + partial_size
		}

		worker.SyncWG.Add(1)
		log.Printf("Starting worker %d: from %d - %d", num, start, end-1)
		go worker.writeRange(num, start, end-1)
		start = end
	}
	if err != nil {
		return err
	}

	worker.SyncWG.Wait()
	killSig <- true
	close(worker.Chunk)
	close(killSig)
	return nil
}

type Worker struct {
	File         *os.File
	RemoteFile   *File
	Count        int64
	SyncWG       sync.WaitGroup
	TotalSize    int64
	WrittenCount int64
	Chunk        chan chunk
	Mut          sync.Mutex
}

type chunk struct {
	Data  []byte
	Part  int
	Start int
}

func (w *Worker) writeFile(done chan bool) {
	for {
		select {
		case part := <-w.Chunk:
			written, err := w.File.WriteAt(part.Data, int64(part.Start))
			if err != nil {
				log.Fatalf("Failed to write chunk for part %d", part.Part)
			}
			w.Mut.Lock()
			w.WrittenCount += int64(written)
			w.Mut.Unlock()
		case <-done:
			log.Printf("Done writing file")
			return
		}
	}
}
func (w *Worker) writeRange(part_num int64, start int64, end int64) {
	//var written int64
	body, err := w.getRangeBody(start, end)
	if err != nil {
		log.Fatalf("Part %d request error: %s\n", part_num, err.Error())
	}

	buff := make([]byte, 1024)
	chunkStart := int(start)
	for {
		numBytes, err := body.Read(buff)
		if err == io.EOF {
			break
		} else if err != nil {
			// TODO: write to error channel
			return
		}
		w.Chunk <- chunk{
			Part:  int(part_num),
			Data:  buff[0:numBytes],
			Start: chunkStart,
		}
	}
	log.Printf("Done writing part %d", part_num)
	defer w.SyncWG.Done()
}

func (w *Worker) getRangeBody(start int64, end int64) (io.ReadCloser, error) {
	handler, err := w.RemoteFile.getObjectHandle()
	if err != nil {
		return nil, err
	}
	reader, err := handler.ObjectHandle().NewRangeReader(context.Background(), start, end)
	if err != nil {
		return nil, err
	}

	return reader, err
}
