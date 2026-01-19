package datapipe_test

import "github.com/nulllvoid/datapipe"

type testEntity struct {
	ID   string
	Name string
}

func (e testEntity) GetID() string { return e.ID }

type testRequest struct {
	Limit      int
	Offset     int
	Query      string
	Auth       string
	Filters    map[string]any
	Sort       string
	Order      datapipe.SortOrder
	ShouldFail bool
}

func (r *testRequest) GetLimit() int                    { return r.Limit }
func (r *testRequest) GetOffset() int                   { return r.Offset }
func (r *testRequest) Clone() datapipe.Request          { copy := *r; return &copy }
func (r *testRequest) GetAuthType() string              { return r.Auth }
func (r *testRequest) GetSearchQuery() string           { return r.Query }
func (r *testRequest) HasSearchQuery() bool             { return r.Query != "" }
func (r *testRequest) GetFilters() map[string]any       { return r.Filters }
func (r *testRequest) GetSortField() string             { return r.Sort }
func (r *testRequest) GetSortOrder() datapipe.SortOrder { return r.Order }
