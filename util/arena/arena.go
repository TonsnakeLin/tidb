// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package arena

import (
	"reflect"
	"unsafe"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/types"
)

type IntSliceAllocator struct {
	slice    []int
	offset   int
	capacity int
}

func (sa *IntSliceAllocator) InitIntSlice() {
	sa.slice = make([]int, 4096, 4096)
	sa.offset = 0
	sa.capacity = 4096
}

func (sa *IntSliceAllocator) GetIntSliceByCap(cap int) []int {
	origOffset := sa.offset
	if origOffset+cap > sa.capacity {
		return make([]int, 0, cap)
	}
	sa.offset += cap
	return sa.slice[origOffset : origOffset : origOffset+cap]
}

func (sa *IntSliceAllocator) GetIntSliceByLen(len int) []int {
	origOffset := sa.offset
	if origOffset+len > sa.capacity {
		return make([]int, len)
	}
	sa.offset += len
	return sa.slice[origOffset : origOffset+len : origOffset+len]
}

func (sa *IntSliceAllocator) Reset() {
	sa.offset = 0
}

type ByteSliceAllocator struct {
	slice    []byte
	offset   int
	capacity int
}

func (sa *ByteSliceAllocator) InitByteSlice() {
	sa.slice = make([]byte, 4096, 4096)
	sa.offset = 0
	sa.capacity = 4096
}

func (sa *ByteSliceAllocator) GetByteSliceByCap(cap int) []byte {
	origOffset := sa.offset
	if origOffset+cap > sa.capacity {
		return make([]byte, 0, cap)
	}
	sa.offset += cap
	return sa.slice[origOffset : origOffset : origOffset+cap]
}

func (sa *ByteSliceAllocator) GetByteSliceByLen(len int) []byte {
	origOffset := sa.offset
	if origOffset+len > sa.capacity {
		return make([]byte, len)
	}
	sa.offset += len
	return sa.slice[origOffset : origOffset+len : origOffset+len]
}

func (sa *ByteSliceAllocator) Reset() {
	sa.offset = 0
}

// Allocator pre-allocates memory to reduce memory allocation cost.
// It is not thread-safe.
type Allocator interface {
	// Alloc allocates memory with 0 len and capacity cap.
	Alloc(capacity int) []byte

	// AllocWithLen allocates memory with length and capacity.
	AllocWithLen(length int, capacity int) []byte

	// Reset resets arena offset.
	// Make sure all the allocated memory are not used any more.
	Reset()
}

// SimpleAllocator is a simple implementation of ArenaAllocator.
type SimpleAllocator struct {
	arena []byte
	off   int
}

type stdAllocator struct {
}

func (*stdAllocator) Alloc(capacity int) []byte {
	return make([]byte, 0, capacity)
}

func (*stdAllocator) AllocWithLen(length int, capacity int) []byte {
	return make([]byte, length, capacity)
}

func (*stdAllocator) Reset() {}

var _ Allocator = &stdAllocator{}

// StdAllocator implements Allocator but do not pre-allocate memory.
var StdAllocator = &stdAllocator{}

// NewAllocator creates an Allocator with a specified capacity.
func NewAllocator(capacity int) *SimpleAllocator {
	return &SimpleAllocator{arena: make([]byte, 0, capacity)}
}

// Alloc implements Allocator.AllocBytes interface.
func (s *SimpleAllocator) Alloc(capacity int) []byte {
	if s.off+capacity < cap(s.arena) {
		slice := s.arena[s.off : s.off : s.off+capacity]
		s.off += capacity
		return slice
	}

	return make([]byte, 0, capacity)
}

// AllocWithLen implements Allocator.AllocWithLen interface.
func (s *SimpleAllocator) AllocWithLen(length int, capacity int) []byte {
	slice := s.Alloc(capacity)
	return slice[:length:capacity]
}

// Reset implements Allocator.Reset interface.
func (s *SimpleAllocator) Reset() {
	s.off = 0
}

// MixedMemPool is used allocate different type objects and slices by allocated memory.
type MixedMemPool struct {
	objAllocator ObjectorAllocator
}

// ObjectorAllocator is a .
type ObjectorAllocator struct {
	arena    []byte
	offset   int
	capacity int
}

func (objAlloc *ObjectorAllocator) Init() {
	objAlloc.arena = make([]byte, 0, 262144)
	objAlloc.offset = 0
	objAlloc.capacity = 262144
}

func (objAlloc *ObjectorAllocator) Reset() {
	objAlloc.offset = 0
	(*reflect.SliceHeader)(unsafe.Pointer(&objAlloc.arena)).Len = 0
}

func (objAlloc *ObjectorAllocator) GetObjectPointer(len int) unsafe.Pointer {
	if objAlloc.offset+len > objAlloc.capacity {
		return nil
	}

	curArena := objAlloc.arena[objAlloc.offset:]
	arenaPtr := unsafe.Pointer(&curArena)
	objPtr := unsafe.Pointer((*reflect.SliceHeader)(arenaPtr).Data)
	// fmt.Println("objPtr:", objPtr, "data pointer: ", (*reflect.SliceHeader)(arenaPtr).Data)

	objAlloc.offset += len
	(*reflect.SliceHeader)(unsafe.Pointer(&objAlloc.arena)).Len = objAlloc.offset
	return objPtr
}

type SliceAlloctor struct {
	ExprSlice       any
	ExprColumnSlice any
	UtilRangeSlice  any
	VisitInfoSlice  any
	IntSlice        *IntSliceAllocator
	ByteSlice       *ByteSliceAllocator
	DatumSlice      *types.DatumSliceAllocator
	FieldTypeSlice  *types.FieldTypeSliceAllocator
	FieldNameSlice  *types.FieldNameSliceAllocator
	ModelColumnInfo *model.ModelColumnInfoSliceAllocator

	TableAliasInJoin []map[string]interface{}
	CteCanUsed       []string
	CteBeforeOffset  []int
}

func (sa *SliceAlloctor) Reset() {
	sa.DatumSlice.Reset()
	sa.FieldTypeSlice.Reset()
	sa.FieldNameSlice.Reset()
	sa.ModelColumnInfo.Reset()
	sa.IntSlice.Reset()
	sa.ByteSlice.Reset()
	sa.TableAliasInJoin = sa.TableAliasInJoin[0:]
	sa.CteCanUsed = sa.CteCanUsed[0:]
	sa.CteBeforeOffset = sa.CteBeforeOffset[0:]
}

func (sa *SliceAlloctor) InitSliceAlloctor() {
	sa.DatumSlice = &types.DatumSliceAllocator{}
	sa.FieldTypeSlice = &types.FieldTypeSliceAllocator{}
	sa.FieldNameSlice = &types.FieldNameSliceAllocator{}
	sa.ModelColumnInfo = &model.ModelColumnInfoSliceAllocator{}
	sa.IntSlice = &IntSliceAllocator{}
	sa.DatumSlice.InitDatumSlice()
	sa.FieldTypeSlice.InitFieldTypeSlice()
	sa.FieldNameSlice.InitFieldNameSlice()
	sa.ModelColumnInfo.InitColumnInfoSlice()
	sa.IntSlice.InitIntSlice()
	sa.ByteSlice.InitByteSlice()

	sa.TableAliasInJoin = make([]map[string]interface{}, 0)
	sa.CteCanUsed = make([]string, 0)
	sa.CteBeforeOffset = make([]int, 0)
}

type MapAllocator struct {
	// StmtCtxsmall maps
	StatsLoadStatus      map[model.TableItemID]string
	LockTableIDs         map[int64]struct{}
	TblInfo2UnionScan    map[*model.TableInfo]bool
	TableStats           map[int64]interface{}
	isolationReadEngines map[kv.StoreType]struct{}
}

func (ma *MapAllocator) InitMapAllocator() {
	ma.StatsLoadStatus = make(map[model.TableItemID]string, 2)
	ma.LockTableIDs = make(map[int64]struct{})
	ma.TblInfo2UnionScan = make(map[*model.TableInfo]bool, 2)
	ma.TableStats = make(map[int64]interface{})
	ma.isolationReadEngines = make(map[kv.StoreType]struct{}, 3)
}

func (ma *MapAllocator) GetTblInfo2UnionScanMap() map[*model.TableInfo]bool {
	for k := range ma.TblInfo2UnionScan {
		delete(ma.TblInfo2UnionScan, k)
	}
	return ma.TblInfo2UnionScan
}

func (ma *MapAllocator) GetStatsLoadStatusMap() map[model.TableItemID]string {
	for k := range ma.StatsLoadStatus {
		delete(ma.StatsLoadStatus, k)
	}
	return ma.StatsLoadStatus
}

func (ma *MapAllocator) GetLockTableIDs() map[int64]struct{} {
	for k := range ma.LockTableIDs {
		delete(ma.LockTableIDs, k)
	}
	return ma.LockTableIDs
}

func (ma *MapAllocator) GetTableStatsMap() map[int64]interface{} {
	for k := range ma.TableStats {
		delete(ma.TableStats, k)
	}
	return ma.TableStats
}

func (ma *MapAllocator) GetIsolationReadEnginesMap() map[kv.StoreType]struct{} {
	for k := range ma.isolationReadEngines {
		delete(ma.isolationReadEngines, k)
	}
	return ma.isolationReadEngines
}

func (ma *MapAllocator) Reset() {
	// do nothing, when getting map, it will reset the map
}
