package gopherql

import "encoding/binary"

// TODO - Huge room for effiencies here but lets get it working
// TODO - Both reader and writer have huge room for improvement
type ByteReader struct {
	Offset   int
	Contents []byte
}

func NewByteReader(contents []byte) *ByteReader {
	return &ByteReader{
		Offset:   0,
		Contents: contents,
	}
}

func (b *ByteReader) ReadUint8() int {
	result := int(b.Contents[b.Offset])
	b.Offset += 1
	return result
}

func (b *ByteReader) ReadByte() byte {
	result := b.Contents[b.Offset]
	b.Offset++
	return result
}

func (b *ByteReader) ReadUint16() int {
	result := binary.BigEndian.Uint16(b.Contents[b.Offset : b.Offset+uint16Size])
	b.Offset += uint16Size
	return int(result)
}

func (b *ByteReader) ReadUint32() int {
	result := binary.BigEndian.Uint32(b.Contents[b.Offset : b.Offset+uint32Size])
	b.Offset += uint32Size
	return int(result)
}

func (b *ByteReader) ReadString(size int) string {
	result := string(b.Contents[b.Offset : b.Offset+size])
	b.Offset += size
	return result
}

func (b *ByteReader) ReadBool() bool {
	result := b.Contents[b.Offset] >= 1
	b.Offset++
	return result
}

func (b *ByteReader) ReadStringAt(start, end int) string {
	return string(b.Contents[start:end])
}

func (b *ByteReader) ReadBytesAt(start, end int) []byte {
	return b.Contents[start:end]
}

func (b *ByteReader) ReadBytes(size int) []byte {
	result := b.Contents[b.Offset : b.Offset+size]
	b.Offset += size
	return result
}

func (b *ByteReader) ReadByteAsBool(val byte) bool {
	if val >= 1 {
		return true
	}
	return false
}

func (b *ByteReader) Advance(step int) {
	b.Offset += step
}

type ByteWriter struct {
	Contents []byte
}

func NewByteWriter() *ByteWriter {
	return &ByteWriter{Contents: []byte{}}
}

func (w *ByteWriter) WriteUint32(val int) {
	contents := make([]byte, 4)
	binary.BigEndian.PutUint32(contents, uint32(val))
	w.Contents = append(w.Contents, contents...)
}

func (w *ByteWriter) WriteUint8(val int) {
	contents := make([]byte, 1)
	contents[0] = uint8(val)
	w.Contents = append(w.Contents, contents...)
}

func (w *ByteWriter) WriteUint16(val int) {
	contents := make([]byte, 2)
	binary.BigEndian.PutUint16(contents, uint16(val))
	w.Contents = append(w.Contents, contents...)
}

func (w *ByteWriter) WriteByte(content byte) {
	w.Contents = append(w.Contents, content)
}

func (w *ByteWriter) WriteBytes(contents []byte) {
	w.Contents = append(w.Contents, contents...)
}

func (w *ByteWriter) WriteString(contents string) {
	w.Contents = append(w.Contents, []byte(contents)...)
}

func (w *ByteWriter) AppendBytes(contents []byte) {
	w.Contents = append(w.Contents, contents...)
}

func (w *ByteWriter) WriteBool(val bool) {
	truthy := make([]byte, 1)
	if val == true {
		truthy[0] = 1
	}
	w.Contents = append(w.Contents, truthy...)
}

func (w *ByteWriter) Bytes() []byte {
	return w.Contents
}
