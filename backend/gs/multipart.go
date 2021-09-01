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
		Readers:    make(chan partResponse, workerCount),
	}

	var start, end int64
	var partial_size = int64(size) / workerCount
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
	close(worker.Readers)

	log.Printf("Done downloading file")
	return nil
}

type Worker struct {
	File       *os.File
	RemoteFile *File
	Count      int64
	SyncWG     sync.WaitGroup
	TotalSize  int64
	Readers    chan partResponse
}

type partResponse struct {
	Body io.ReadCloser
	Part int64
}

func (w *Worker) writeRange(part_num int64, start int64, end int64) {
	//var written int64
	body, err := w.getRangeBody(start, end)
	if err != nil {
		log.Fatalf("Part %d request error: %s\n", part_num, err.Error())
	}
	w.Readers <- partResponse{
		Part: part_num,
		Body: body,
	}
	defer w.SyncWG.Done()
	//
	//// make a buffer to keep chunks that are read
	//buf := make([]byte, 4*1024)
	//for {
	//	nr, er := body.Read(buf)
	//	if nr > 0 {
	//		nw, err := w.File.WriteAt(buf[0:nr], start)
	//		if err != nil {
	//			log.Fatalf("Part %d occured error: %s.\n", part_num, err.Error())
	//		}
	//		if nr != nw {
	//			log.Fatalf("Part %d occured error of short writiing.\n", part_num)
	//		}
	//
	//		start = int64(nw) + start
	//		if nw > 0 {
	//			written += int64(nw)
	//		}
	//	}
	//	if er != nil {
	//		if er.Error() == "EOF" {
	//			log.Printf("Part %d done.\n", part_num)
	//			break
	//		}
	//		handleError(errors.New(fmt.Sprintf("Part %d occured error: %s\n", part_num, er.Error())))
	//	}
	//}
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

func handleError(err error) {
	if err != nil {
		log.Println("err:", err)
		os.Exit(1)
	}
}
