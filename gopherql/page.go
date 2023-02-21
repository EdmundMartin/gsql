package gopherql

import (
	"bytes"
	"encoding/binary"
)

type PageObject struct {
	Key           []byte
	Value         []byte
	IsBlobRef     bool
	TransactionID uint32
	DeleteID      uint32
}


func NewPageObject(key, value []byte, tid, xid uint32) *PageObject {
	return &PageObject{
		Key:           key,
		Value:         value,
		IsBlobRef:     false,
		TransactionID: tid,
		DeleteID:      xid,
	}
}

func blobObjectKey(key []byte, part uint32) []byte {
	buffer := bytes.Buffer{}

	buffer.WriteByte('B')
	buffer.Write(key)
	buffer.WriteByte('0')
	buff := make([]byte, 4)
	binary.BigEndian.PutUint32(buff, part)
	buffer.Write(buff)

	return buffer.Bytes()
}

func NewBlobPageObject(key, value []byte, tid, xid, part uint32) *PageObject {
	return &PageObject{
		Key: blobObjectKey(key, part),
		Value: value,
		IsBlobRef: false,
		TransactionID: tid,
		DeleteID: xid,
	}
}

func newBlobFragmentKey(key []byte) []byte {
	buffer := bytes.Buffer{}
	buffer.WriteByte('F')
	buffer.Write(key)

	return buffer.Bytes()
}

func NewFragmentPageObject(key, value []byte, tid, xid uint32) *PageObject {
	return &PageObject{
		Key:           newBlobFragmentKey(key),
		Value:         value,
		IsBlobRef:     false,
		TransactionID: tid,
		DeleteID:      xid,
	}
}

func NewReferencePageObject(key []byte, tid, xid, pieces uint32, hasFrag bool) *PageObject {
	buffer := bytes.Buffer{}
	contents := make([]byte, 4)
	binary.BigEndian.PutUint32(contents, pieces)
	buffer.Write(contents)

	if hasFrag == true {
		buffer.WriteByte(1)
	} else {
		buffer.WriteByte(0)
	}

	return &PageObject{
		Key:           key,
		Value:         buffer.Bytes(),
		IsBlobRef:     true,
		TransactionID: tid,
		DeleteID:      xid,
	}
}