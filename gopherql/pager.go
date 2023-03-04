package gopherql

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
)

type Pager interface {
	FetchPage(num int) (*Page, error)
	StorePage(num int, p *Page) error
	AppendPage(page *Page) (int, error)
	TruncateAll() error
	TruncateLastPage() error
	TotalPages() int
	GetRootPage() int
	SetRootPage(num int) error
}

type MemoryPager struct {
	RootPage int
	Pages    []*Page
}

func (m *MemoryPager) FetchPage(num int) (*Page, error) {
	if num >= len(m.Pages) {
		return nil, errors.New("page out of idx")
	}
	return m.Pages[num], nil
}

func (m *MemoryPager) StorePage(num int, p *Page) error {
	if num >= len(m.Pages) {
		return errors.New("page out of idx")
	}
	m.Pages[num] = p
	return nil
}

func (m *MemoryPager) AppendPage(page *Page) (int, error) {
	m.Pages = append(m.Pages, page)
	return len(m.Pages) - 1, nil
}

func (m *MemoryPager) TruncateAll() error {
	m.Pages = []*Page{}
	return nil
}

func (m *MemoryPager) TruncateLastPage() error {
	if len(m.Pages) == 0 {
		return errors.New("page out of idx")
	}
	m.Pages = m.Pages[:len(m.Pages)-1]
	return nil
}

func (m *MemoryPager) TotalPages() int {
	return len(m.Pages)
}

func (m *MemoryPager) GetRootPage() int {
	return m.RootPage
}

func (m *MemoryPager) SetRootPage(num int) error {
	m.RootPage = num
	return nil
}

func NewMemoryPager() *MemoryPager {
	return &MemoryPager{}
}

type FilePager struct {
	pageSize   int
	file       *os.File
	totalPages int
	rootPage   int
}

func NewFilePager(file *os.File, pageSize int, rootPage int) (*FilePager, error) {

	contents, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}

	totalPages := len(contents) / defaultPgSize
	return &FilePager{
		pageSize:   defaultPgSize,
		file:       file,
		totalPages: totalPages,
		rootPage:   rootPage,
	}, nil
}

func (fp *FilePager) FetchPage(num int) (*Page, error) {

	start := int64(defaultPgSize + (defaultPgSize * num))
	if _, err := fp.file.Seek(start, 0); err != nil {
		return nil, err
	}
	fmt.Println(start)

	buffer := make([]byte, defaultPgSize)

	if _, err := fp.file.Read(buffer); err != nil {
		return nil, err
	}

	nb := NewByteReader(buffer)
	return &Page{
		Kind: nb.ReadByte(),
		Used: uint16(nb.ReadUint16()),
		Data: nb.ReadBytes(defaultPgSize - pageHeaderSize),
	}, nil
}

func (fp *FilePager) StorePage(num int, p *Page) error {

	start := int64(defaultPgSize + (defaultPgSize * num))
	if _, err := fp.file.Seek(start, 0); err != nil {
		return err
	}

	bWriter := NewByteWriter()
	bWriter.WriteByte(p.Kind)
	bWriter.WriteUint16(int(p.Used))
	bWriter.WriteBytes(p.Data)

	if _, err := fp.file.Write(bWriter.Bytes()); err != nil {
		return err
	}
	return fp.file.Sync()
}

func (fp *FilePager) AppendPage(page *Page) (int, error) {
	if err := fp.StorePage(fp.totalPages, page); err != nil {
		return -1, err
	}
	fp.totalPages++

	return fp.totalPages - 1, nil
}

func (fp *FilePager) TruncateAll() error {
	fp.totalPages = 0
	return nil
}

func (fp *FilePager) TruncateLastPage() error {
	fp.totalPages--
	return nil
}

func (fp *FilePager) TotalPages() int {
	return fp.totalPages - 1
}

func (fp *FilePager) GetRootPage() int {
	return fp.rootPage
}

func (fp *FilePager) SetRootPage(num int) error {
	fp.rootPage = num
	return nil
}
