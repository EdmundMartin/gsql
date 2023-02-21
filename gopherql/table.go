package gopherql

import (
	"bytes"
	"fmt"
)

// TODO - Go generate - plus do we need richer types?
const (
	StringColumn ColumnType = iota
	BoolColumn
	Int64Column
)

const uint16Size = 2
const uint32Size = 4

type ColumnType uint8

type Column struct {
	Name    string
	Type    ColumnType
	NotNull bool
}

func (c *Column) Bytes() []byte {

	bWriter := NewByteWriter()

	bWriter.WriteUint32(len(c.Name))
	bWriter.WriteString(c.Name)
	bWriter.WriteUint8(int(c.Type))
	bWriter.WriteBool(c.NotNull)

	return bWriter.Bytes()
}

func ColumnFromBytes(contents []byte) (*Column, error) {

	c := &Column{}

	breader := NewByteReader(contents)

	nameSize := breader.ReadUint32()

	c.Name = breader.ReadString(nameSize)
	c.Type = ColumnType(breader.ReadUint8())
	c.NotNull = breader.ReadByteAsBool(contents[len(contents)-1])

	return c, nil
}

func (c *Column) String() string {
	return fmt.Sprintf("%s %d", c.Name, c.Type)
}

type Columns []*Column

func (c Columns) String() string {
	// TODO Maybe refactor
	buffer := bytes.Buffer{}

	for _, c := range c {
		buffer.WriteString(c.String())
		buffer.WriteString(",")
	}
	return buffer.String()
}

func (c Columns) Bytes() []byte {

	bwriter := NewByteWriter()

	bwriter.WriteUint8(len(c))

	colWriter := NewByteWriter()
	for _, col := range c {
		column := col.Bytes()
		colWriter.AppendBytes(column)
		bwriter.WriteUint32(len(column))
	}

	return bytes.Join([][]byte{
		bwriter.Bytes(),
		colWriter.Bytes(),
	}, nil)
}

func ColumnsFromBytes(contents []byte) (Columns, error) {

	breader := NewByteReader(contents)
	columnCount := breader.ReadUint8()

	cols := make([]*Column, columnCount)
	contentOffset := breader.Offset + (uint32Size * columnCount)
	for i := 0; i < columnCount; i++ {
		sizeContent := breader.ReadUint32()

		col, err := ColumnFromBytes(breader.ReadBytesAt(contentOffset, contentOffset+sizeContent))
		if err != nil {
			return nil, err
		}
		cols[i] = col
		contentOffset += sizeContent
	}
	return cols, nil
}

type PrimaryKeys []string

func (p PrimaryKeys) Bytes() []byte {

	bwriter := NewByteWriter()

	bwriter.WriteUint8(len(p))

	keyWriter := NewByteWriter()
	for _, key := range p {
		bwriter.WriteUint32(len(key))
		keyWriter.WriteString(key)
	}

	return bytes.Join([][]byte{
		bwriter.Bytes(),
		keyWriter.Bytes(),
	}, nil)
}

func PrimaryKeysFromBytes(contents []byte) PrimaryKeys {
	breader := NewByteReader(contents)

	numberPks := breader.ReadUint8()
	pks := make([]string, numberPks)
	contentOffset := (uint32Size * numberPks) + 1

	for idx := 0; idx < numberPks; idx++ {
		size := breader.ReadUint32()
		pks[idx] = breader.ReadStringAt(contentOffset, contentOffset+size)
		contentOffset += size
	}
	return pks
}

type Table struct {
	Name        string
	Columns     Columns
	PrimaryKeys PrimaryKeys
	Virtual     bool
}

func (t *Table) ColumnNames() []string {
	result := make([]string, len(t.Columns))
	for idx, col := range t.Columns {
		result[idx] = col.Name
	}
	return result
}

func (t *Table) Column(name string) (*Column, error) {

	for _, c := range t.Columns {
		if name == c.Name {
			return c, nil
		}
	}
	return nil, SQLStateError{Code: "42703", Msg: "Column does not exist"}
}

func (t *Table) Bytes() []byte {

	bwriter := NewByteWriter()

	bwriter.WriteUint32(len(t.Name))
	bwriter.WriteString(t.Name)

	columns := t.Columns.Bytes()
	bwriter.WriteUint32(len(columns))
	bwriter.AppendBytes(columns)

	pks := t.PrimaryKeys.Bytes()
	bwriter.WriteUint32(len(pks))
	bwriter.AppendBytes(pks)

	bwriter.WriteBool(t.Virtual)

	return bwriter.Bytes()
}

func TableFromBytes(contents []byte) (*Table, error) {
	t := &Table{}

	reader := NewByteReader(contents)
	tableNameLength := reader.ReadUint32()
	t.Name = reader.ReadString(tableNameLength)

	columnSize := reader.ReadUint32()
	cols, err := ColumnsFromBytes(contents[reader.Offset : reader.Offset+columnSize])
	if err != nil {
		return nil, err
	}
	t.Columns = cols
	reader.Advance(columnSize)

	pkSize := reader.ReadUint32()
	pks := PrimaryKeysFromBytes(contents[reader.Offset : reader.Offset+pkSize])
	t.PrimaryKeys = pks
	reader.Advance(pkSize)

	t.Virtual = reader.ReadByteAsBool(contents[len(contents)-1])

	return t, nil
}

type TableOperation struct {
	// TODO
}
