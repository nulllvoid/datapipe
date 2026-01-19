package datapipe

type Response[T Entity] interface {
	GetItems() []T
	GetCount() int
	HasMore() bool
}

type CollectionResponse[T Entity] struct {
	Entity string `json:"entity"`
	Count  int    `json:"count"`
	More   bool   `json:"has_more"`
	Items  []T    `json:"items"`
}

func (r *CollectionResponse[T]) GetItems() []T { return r.Items }
func (r *CollectionResponse[T]) GetCount() int { return r.Count }
func (r *CollectionResponse[T]) HasMore() bool { return r.More }

func NewCollectionResponse[T Entity](items []T, hasMore bool) *CollectionResponse[T] {
	return &CollectionResponse[T]{
		Entity: "collection",
		Count:  len(items),
		More:   hasMore,
		Items:  items,
	}
}

