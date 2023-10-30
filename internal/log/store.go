package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)


/*ze Metadata: In your code, before writing the actual data p to the Store,
 you first write the size of the data as metadata. This size metadata indicates 
 how many bytes of actual data follow it.
Consistency: To ensure that the size metadata is consistently interpreted,
 you need to agree on a fixed width for it. 
 By using lenWidth, you specify that the size metadata is a fixed number of bytes. 
 This ensures that when you read from the file, you know exactly how many bytes to read to extract the size information.
*/
const (
	lenWidth = 8
)

type Store struct {
	*os.File
	size uint64
	mu     sync.Mutex
	buf *bufio.Writer
}

func newStore(file *os.File)(*Store, error){
	fi, err := os.Stat(file.Name())
	if err!= nil{	
		return nil, err
	}
	size := uint64(fi.Size())
	return &Store{
		File: file,
		size: size,
		buf: bufio.NewWriter(file),
	}, nil

}

func (s *Store)Append(p []byte)(n uint64, pos uint64, err error){
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size
	if err :=  binary.Write(s.buf,enc, uint64(len(p))); err != nil{
		return 0, 0, err
	}
	w, err := s.buf.Write(p)
	if err!= nil{
		return 0, 0, err
	}
	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}


func (s *Store)ReadAt(p []byte, off int64)(int ,error){
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err !=nil{
		return 0, err
	}
	return s.File.ReadAt(p, off)

}

func (s *Store)Read(pos uint64)([]byte, error){
	if err := s.buf.Flush(); err !=nil{
		return nil, err
	}
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil{
		return nil, err
	}
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil{
		return nil, err
	}
	return b, nil
}

func (s *Store)Close() error{
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err!= nil{
		return err
	}
	return s.File.Close()
}