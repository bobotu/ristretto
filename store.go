/*
 * Copyright 2019 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ristretto

import (
	"sync"
)

// store is the interface fulfilled by all hash map implementations in this
// file. Some hash map implementations are better suited for certain data
// distributions than others, so this allows us to abstract that out for use
// in Ristretto.
//
// Every store is safe for concurrent usage.
type store interface {
	// Get returns the value associated with the key parameter.
	Get(uint64) (interface{}, bool)
	// Set adds the key-value pair to the Map or updates the value if it's
	// already present.
	Set(uint64, interface{})
	// Del deletes the key-value pair from the Map.
	Del(uint64) interface{}
	// Update attempts to update the key with a new value and returns true if
	// successful.
	Update(uint64, interface{}) bool
	// Clear clears all contents of the store.
	Clear()
}

// newStore returns the default store implementation.
func newStore() store {
	return newShardedMap()
}

const numShards uint64 = 256

type shardedMap struct {
	shards []*lockedMap
}

func newShardedMap() *shardedMap {
	sm := &shardedMap{
		shards: make([]*lockedMap, int(numShards)),
	}
	for i := range sm.shards {
		sm.shards[i] = newLockedMap()
	}
	return sm
}

func (sm *shardedMap) Get(key uint64) (interface{}, bool) {
	return sm.shards[key%numShards].Get(key)
}

func (sm *shardedMap) Set(key uint64, value interface{}) {
	sm.shards[key%numShards].Set(key, value)
}

func (sm *shardedMap) Del(key uint64) interface{} {
	return sm.shards[key%numShards].Del(key)
}

func (sm *shardedMap) Update(key uint64, value interface{}) bool {
	return sm.shards[key%numShards].Update(key, value)
}

func (sm *shardedMap) Clear() {
	for i := uint64(0); i < numShards; i++ {
		sm.shards[i].Clear()
	}
}

type lockedMap struct {
	sync.RWMutex
	data map[uint64]interface{}
}

func newLockedMap() *lockedMap {
	return &lockedMap{
		data: make(map[uint64]interface{}),
	}
}

func (m *lockedMap) Get(key uint64) (interface{}, bool) {
	m.RLock()
	item, ok := m.data[key]
	m.RUnlock()
	return item, ok
}

func (m *lockedMap) Set(key uint64, value interface{}) {
	m.Lock()
	m.data[key] = value
	m.Unlock()
}

func (m *lockedMap) Del(key uint64) interface{} {
	m.Lock()
	item, ok := m.data[key]
	if ok {
		delete(m.data, key)
	}
	m.Unlock()
	return item
}

func (m *lockedMap) Update(key uint64, value interface{}) bool {
	m.Lock()

	if _, ok := m.data[key]; !ok {
		m.Unlock()
		return false
	}
	m.data[key] = value
	m.Unlock()
	return true
}

func (m *lockedMap) Clear() {
	m.Lock()
	m.data = make(map[uint64]interface{})
	m.Unlock()
}
