package ristretto

import (
	"testing"
)

func TestStoreSetGet(t *testing.T) {
	s := newStore()
	key := uint64(1)
	s.Set(key, 2)
	if val, ok := s.Get(key); (val == nil || !ok) || val.(int) != 2 {
		t.Fatal("set/get error")
	}
	s.Set(key, 3)
	if val, ok := s.Get(key); (val == nil || !ok) || val.(int) != 3 {
		t.Fatal("set/get overwrite error")
	}
	key = uint64(2)
	s.Set(key, 2)
	if val, ok := s.Get(key); !ok || val.(int) != 2 {
		t.Fatal("set/get nil key error")
	}
}

func TestStoreDel(t *testing.T) {
	s := newStore()
	key := uint64(1)
	s.Set(key, 1)
	s.Del(key)
	if val, ok := s.Get(key); val != nil || ok {
		t.Fatal("del error")
	}
	s.Del(2)
}

func TestStoreClear(t *testing.T) {
	s := newStore()
	for i := uint64(0); i < 1000; i++ {
		key := uint64(i)
		s.Set(key, i)
	}
	s.Clear()
	for i := uint64(0); i < 1000; i++ {
		key := uint64(i)
		if val, ok := s.Get(key); val != nil || ok {
			t.Fatal("clear operation failed")
		}
	}
}

func TestStoreUpdate(t *testing.T) {
	s := newStore()
	key := uint64(1)
	s.Set(key, 1)
	if updated := s.Update(key, 2); !updated {
		t.Fatal("value should have been updated")
	}
	if val, ok := s.Get(key); val == nil || !ok {
		t.Fatal("value was deleted")
	}
	if val, ok := s.Get(key); val.(int) != 2 || !ok {
		t.Fatal("value wasn't updated")
	}
	if !s.Update(key, 3) {
		t.Fatal("value should have been updated")
	}
	if val, ok := s.Get(key); val.(int) != 3 || !ok {
		t.Fatal("value wasn't updated")
	}
	key = uint64(2)
	if updated := s.Update(key, 2); updated {
		t.Fatal("value should not have been updated")
	}
	if val, ok := s.Get(key); val != nil || ok {
		t.Fatal("value should not have been updated")
	}
}

func BenchmarkStoreGet(b *testing.B) {
	s := newStore()
	key := uint64(1)
	s.Set(key, 1)
	b.SetBytes(1)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Get(key)
		}
	})
}

func BenchmarkStoreSet(b *testing.B) {
	s := newStore()
	key := uint64(1)
	b.SetBytes(1)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Set(key, 1)
		}
	})
}

func BenchmarkStoreUpdate(b *testing.B) {
	s := newStore()
	key := uint64(1)
	s.Set(key, 1)
	b.SetBytes(1)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Update(key, 2)
		}
	})
}
