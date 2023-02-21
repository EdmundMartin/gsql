package gopherql

import (
	"encoding/binary"
	"os"
)

const (
	currentVersion = 1
	pageSize       = 4096
)

type Header struct {
	Version       uint16
	SchemaVersion uint32
	PageSize      uint16
	RootPage      uint32
	TransactionID uint32
	// TODO - Active Transaction IDs
}

func (h *Header) Bytes() []byte {
	page := make([]byte, pageSize)

	binary.BigEndian.PutUint16(page[0:2], h.Version)
	binary.BigEndian.PutUint32(page[2:6], h.SchemaVersion)
	binary.BigEndian.PutUint16(page[6:8], h.PageSize)
	binary.BigEndian.PutUint32(page[8:12], h.RootPage)
	binary.BigEndian.PutUint32(page[12:16], h.TransactionID)

	return page
}

func HeaderFromBytes(contents []byte) *Header {
	h := &Header{}
	bReader := NewByteReader(contents)
	h.Version = uint16(bReader.ReadUint16())
	h.SchemaVersion = uint32(bReader.ReadUint32())
	h.PageSize = uint16(bReader.ReadUint16())
	h.RootPage = uint32(bReader.ReadUint32())
	h.TransactionID = uint32(bReader.ReadUint32())
	return h
}

func NewHeader() *Header {
	return &Header{
		Version:       currentVersion,
		SchemaVersion: 0,
		PageSize:      pageSize,
		RootPage:      0,
		TransactionID: 2,
	}
}

func NewDatabaseFile(path string) error {
	header := NewHeader()

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(header.Bytes())
	if err != nil {
		return err
	}

	return file.Sync()
}

func ReadHeader(file *os.File) (*Header, error) {
	contents := make([]byte, pageSize)
	_, err := file.Read(contents)
	if err != nil {
		return nil, err
	}
	return HeaderFromBytes(contents), nil
}

func WriteHeader(file *os.File, header *Header) error {
	_, err := file.Write(header.Bytes())
	return err
}
