package gopherql

import "errors"

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
	Pages []*Page
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
