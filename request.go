package datapipe

type Request interface {
	GetLimit() int
	GetOffset() int
	Clone() Request
}

type AuthenticatedRequest interface {
	Request
	GetAuthType() string
}

type SearchableRequest interface {
	Request
	GetSearchQuery() string
	HasSearchQuery() bool
}

type FilterableRequest interface {
	Request
	GetFilters() map[string]any
}

type SortableRequest interface {
	Request
	GetSortField() string
	GetSortOrder() SortOrder
}

type SortOrder string

const (
	SortAsc  SortOrder = "asc"
	SortDesc SortOrder = "desc"
)

