package gopherql

import (
	"bytes"
	"encoding/binary"
	"sort"
)

// TODO - Most of this should be private - once we are done lets go through and make stuff private

const pageObjectPrefixLength = 15

type PageObject struct {
	Key           []byte
	Value         []byte
	IsBlobRef     bool
	TransactionID uint32
	DeleteID      uint32
}

type PageObjects []*PageObject

func (p PageObjects) Len() int {
	return len(p)
}

func (p PageObjects) Less(i, j int) bool {
	return bytes.Compare(p[i].Key, p[j].Key) < 0
}

func (p PageObjects) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
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

func (po PageObject) Length() int {
	return pageObjectPrefixLength + len(po.Key) + len(po.Value)
}

func (po PageObject) BlobInfo() (int, bool) {

	bReader := NewByteReader(po.Value)
	blobPieces := bReader.ReadUint32()
	hasFragment := bReader.ReadBool()

	return blobPieces, hasFragment
}

func (po PageObject) Bytes() []byte {

	bWriter := NewByteWriter()
	bWriter.WriteUint32(po.Length())
	bWriter.WriteUint32(int(po.TransactionID))
	bWriter.WriteUint32(int(po.DeleteID))
	bWriter.WriteUint16(len(po.Key))
	bWriter.WriteBytes(po.Key)
	bWriter.WriteBool(po.IsBlobRef)
	bWriter.WriteBytes(po.Value)

	return bWriter.Bytes()
}


func PageObjectFromBytes(data []byte) (int, PageObject) {

	bReader := NewByteReader(data)
	totalLength := bReader.ReadUint32()
	transID := bReader.ReadUint32()
	deleteID := bReader.ReadUint32()

	keyLength := bReader.ReadUint16()
	key := bReader.ReadBytes(keyLength)
	isBlobRef := bReader.ReadBool()

	value := bReader.ReadBytes(totalLength - pageObjectPrefixLength - keyLength)

	return totalLength, PageObject{Key: key, Value: value, IsBlobRef: isBlobRef, TransactionID: uint32(transID), DeleteID: uint32(deleteID)}
}

const pageHeaderSize = 3

type Page struct {
	Kind byte
	Used uint16
	Data []byte
}

func NewPage(kind byte, size int) *Page {
	return &Page{
		Kind: kind,
		Used: pageHeaderSize,
		Data: make([]byte, size - pageHeaderSize),
	}
}

func (p *Page) IsEmpty() bool {
	return p.Used == pageHeaderSize
}

func (p *Page) Size() int {
	return len(p.Data) + pageHeaderSize
}

func (p *Page) Update(old PageObject, tid int) {
	objects := p.Objects()
	oldVersions := p.Versions(old.Key, objects)

	if len(oldVersions) == 1 {
		p.Expire(old.Key, oldVersions[0], tid)
	}

	if len(oldVersions) == 2 {
		p.Delete(old.Key, tid)
	}

}

func (p *Page) Add(obj *PageObject) error {
	
	if int(p.Used) + obj.Length() > p.Size() {
		panic("page cannot fit object")
	}
	
	objects := p.Objects()
	if len(p.Versions(obj.Key, objects)) >= 2 {
		return SQLStateError{
			Code: "40001",
			Msg:  "avoiding concurrent write on individual row",
		}
	}

	objects = append(objects, obj)
	sort.Sort(PageObjects(objects))

	offset := 0

	for _, object := range objects {
		s := object.Bytes()

		for idx := range s {
			p.Data[offset] = s[idx]
			offset++
		}
	}

	p.Used += uint16(obj.Length())

	return nil
}


func (p *Page) Replace(key []byte, transID int, value []byte) error {
	p.Delete(key, transID)
	obj := NewPageObject(key, value, uint32(transID), 0)
	return p.Add(obj)
}

func (p *Page) Keys() [][]byte {
	objects := p.Objects()

	keys := make([][]byte, len(objects))

	for idx, obj := range objects {
		keys[idx] = obj.Key
	}

	return keys
}

func (p *Page) Delete(key []byte, transID int) bool {
	offset := 0
	didDelete := false
	
	for _, obj := range p.Objects() {
		if bytes.Compare(key, obj.Key) == 0 && obj.TransactionID == uint32(transID) {
			p.Used -= uint16(obj.Length())
			didDelete = true
			continue
		}
		
		s := obj.Bytes()
		for idx := range s {
			p.Data[offset] = s[idx]
			offset++
		}
	}
	return didDelete
}

func (p *Page) Expire(key []byte, transID int, deleteID int) bool {
	
	offset := 0
	modified := false
	
	for _, obj := range p.Objects() {
		
		if bytes.Compare(key, obj.Key) == 0 && obj.TransactionID == uint32(transID)  {
			obj.DeleteID = uint32(deleteID)
			modified = true
		}
		
		s := obj.Bytes()
		for idx := range s {
			p.Data[offset] = s[idx]
			offset++
		}
		
	}
	return modified
}

func (p *Page) Get(key []byte, transID int) *PageObject {

	for _, obj := range p.Objects() {
		if bytes.Compare(key, obj.Key) == 0 && obj.TransactionID == uint32(transID) {
			return obj
		}
	}
	return nil
}

func (p *Page) Objects() []*PageObject {
	var objects []*PageObject
	var n uint16

	for n < p.Used - pageHeaderSize {
		m, object := PageObjectFromBytes(p.Data[n:])
		objects = append(objects, &object)
		n += uint16(m)
	}

	return objects
}

func (p *Page) Versions(key []byte, objs []*PageObject) []int {
	var versions []int

	for _, obj := range objs {
		if bytes.Compare(obj.Key, key) == 0 {
			versions = append(versions, int(obj.TransactionID))
		}
	}

	return versions
}