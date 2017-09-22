package eventmaster

import (
	"testing"
)

func TestPopulateTestData(t *testing.T) {
	mds := &mockDataStore{}
	store, err := GetTestEventStore(mds)
	if err != nil {
		t.Fatalf("creating event store: %v", err)
	}

	if err := PopulateTestData(store); err != nil {
		t.Fatalf("populating test data: %v", err)
	}

	dcs, err := store.GetDCs()
	if err != nil {
		t.Fatalf("get dcs: %v", err)
	}
	if got, want := len(dcs), 5; got != want {
		t.Fatalf("got: %v, want %v", got, want)
	}
	t.Logf("%v", dcs)

	topics, err := store.GetTopics()
	if err != nil {
		t.Fatalf("get topics: %v", err)
	}
	if got, want := len(topics), 5; got != want {
		t.Fatalf("got: %v, want %v", got, want)
	}
	t.Logf("%v", topics)
}
