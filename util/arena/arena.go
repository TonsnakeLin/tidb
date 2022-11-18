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
	"sync"
	"time"
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

type MemPoolSet struct {
	mutex          sync.Mutex
	ObjAllocator   *ObjectorAllocator
	SliceAllocator *SliceAlloctor
	MapAlloctor    *MapAllocator
}

func (mps *MemPoolSet) ResetMemPoolSet() {
	mps.ObjAllocator.Reset()
	mps.SliceAllocator.Reset()
	mps.MapAlloctor.Reset()
}

func NewMemPoolSet() *MemPoolSet {
	objAllocator := &ObjectorAllocator{}
	objAllocator.Init()

	sliceAllocator := &SliceAlloctor{}
	sliceAllocator.InitSliceAlloctor()

	mapAllocator := &MapAllocator{}
	mapAllocator.InitMapAllocator()

	return &MemPoolSet{
		ObjAllocator:   objAllocator,
		SliceAllocator: sliceAllocator,
		MapAlloctor:    mapAllocator,
	}
}

//****************************************************************************
// map pool interface
//*****************************************************************************
// miscMapPool
func (mps *MemPoolSet) GetIsolationReadEnginesMap() map[kv.StoreType]struct{} {
	return mps.MapAlloctor.miscMaps.getIsolationReadEnginesMap()
}

func (mps *MemPoolSet) GetTableStatsMap() map[int64]interface{} {
	return mps.MapAlloctor.miscMaps.getTableStatsMap()
}

func (mps *MemPoolSet) GetLockTableIDsMap() map[int64]struct{} {
	return mps.MapAlloctor.miscMaps.getLockTableIDs()
}

func (mps *MemPoolSet) GetStatsLoadStatusMap() map[model.TableItemID]string {
	return mps.MapAlloctor.miscMaps.getStatsLoadStatusMap()
}

func (mps *MemPoolSet) GetTblInfo2UnionScanMap() map[*model.TableInfo]bool {
	return mps.MapAlloctor.miscMaps.getTblInfo2UnionScanMap()
}

//**********************************************************************************
// StringToDurationMapPool
//**********************************************************************************
func (mps *MemPoolSet) GetStringToDurationMap() map[string]time.Duration {
	return mps.MapAlloctor.strToDurationMaps.GetOneMap()
}

// ObjAllocator interface
// ObjAllocator interface
func (mps *MemPoolSet) GetObjectPointer(len int, useCache bool) unsafe.Pointer {
	if !useCache {
		return nil
	}
	return mps.ObjAllocator.GetObjectPointer(len)
}

// Slices interfaces which have no type
func (mps *MemPoolSet) GetExprSlices() any {
	return mps.SliceAllocator.ExprSlices
}

func (mps *MemPoolSet) GetExprCloumnSlice() any {
	return mps.SliceAllocator.ExprColSlices
}

func (mps *MemPoolSet) GetUtilRangeSlice() any {
	return mps.SliceAllocator.UtilRangeSlice
}

func (mps *MemPoolSet) GetVisitInfoSlice() any {
	return mps.SliceAllocator.VisitInfoSlices
}

// Slices interfaces which have definite type
func (mps *MemPoolSet) GetDatumSliceByCap(cap int) []types.Datum {
	return mps.SliceAllocator.DatumSlices.GetDatumSliceByCap(cap)
}

func (mps *MemPoolSet) GetDatumSliceByLen(len int) []types.Datum {
	return mps.SliceAllocator.DatumSlices.GetDatumSliceByLen(len)
}

/*

func (mps *MemPoolSet) GetFldTypeSliceByCap(cap int) []*types.FieldType {
	// return mps.SliceAllocator.FieldTypeSlice.GetFldTypeSliceByCap(cap)
}

func (mps *MemPoolSet) GetFldTypeSliceByLen(len int) []*types.FieldType {
	// return mps.SliceAllocator.FieldTypeSlice.GetFldTypeSliceByLen(len)
}

func (mps *MemPoolSet) GetFldNameSliceByCap(cap int) []*types.FieldName {
	// return mps.SliceAllocator.FieldNameSlice.GetFldNameSliceByCap(cap)
}

func (mps *MemPoolSet) GetFldNameSliceByLen(len int) []*types.FieldName {
	// return mps.SliceAllocator.FieldNameSlice.GetFldNameSliceByLen(len)
}

func (mps *MemPoolSet) GetModelColumnInfoSliceByCap(cap int) []*model.ColumnInfo {
	return mps.SliceAllocator.ModelColumnInfo.GetColumnInfoSliceByCap(cap)
}

func (mps *MemPoolSet) GetModelColumnInfoSliceByLen(len int) []*model.ColumnInfo {
	return mps.SliceAllocator.ModelColumnInfo.GetColumnInfoSliceByLen(len)
}

func (mps *MemPoolSet) GetIntSliceByCap(cap int) []int {
	return mps.SliceAllocator.IntSlice.GetIntSliceByCap(cap)
}

func (mps *MemPoolSet) GetIntSliceByLen(len int) []int {
	return mps.SliceAllocator.IntSlice.GetIntSliceByLen(len)
}

func (mps *MemPoolSet) GetByteSliceByCap(cap int) []byte {
	return mps.SliceAllocator.ByteSlice.GetByteSliceByCap(cap)
}

func (mps *MemPoolSet) GetByteSliceByLen(len int) []byte {
	return mps.SliceAllocator.ByteSlice.GetByteSliceByCap(len)
}
*/
// ObjectorAllocator is a .
type ObjectorAllocator struct {
	mutex    sync.Mutex
	arena    []byte
	offset   int
	capacity int
}

func (objAlloc *ObjectorAllocator) Init() {
	objAlloc.arena = make([]byte, 262144, 262144)
	objAlloc.offset = 0
	objAlloc.capacity = 262144
}

func (objAlloc *ObjectorAllocator) Reset() {
	objAlloc.offset = 0
	// (*reflect.SliceHeader)(unsafe.Pointer(&objAlloc.arena)).Len = 0
}

func (objAlloc *ObjectorAllocator) GetObjectPointer(len int) unsafe.Pointer {
	objAlloc.mutex.Lock()
	defer objAlloc.mutex.Unlock()
	if objAlloc.offset+len > objAlloc.capacity {
		return nil
	}

	curArena := objAlloc.arena[objAlloc.offset:]
	arenaPtr := unsafe.Pointer(&curArena)
	objPtr := unsafe.Pointer((*reflect.SliceHeader)(arenaPtr).Data)
	// fmt.Println("objPtr:", objPtr, "data pointer: ", (*reflect.SliceHeader)(arenaPtr).Data)

	objAlloc.offset += len
	// (*reflect.SliceHeader)(unsafe.Pointer(&objAlloc.arena)).Len = objAlloc.offset
	return objPtr
}

type SliceAlloctor struct {
	ExprSlices      any
	ExprColSlices   any
	UtilRangeSlice  any
	VisitInfoSlices any
	DatumSlices     *types.DatumSlicePool
	/*
		IntSlice        *IntSliceAllocator
		ByteSlice       *ByteSliceAllocator

		FieldTypeSlice  *types.FieldTypeSliceAllocator
		FieldNameSlice  *types.FieldNameSliceAllocator
		ModelColumnInfo *model.ModelColumnInfoSliceAllocator

		TableAliasInJoin []map[string]interface{}
		CteCanUsed       []string
		CteBeforeOffset  []int
	*/
}

func (sa *SliceAlloctor) Reset() {
	sa.DatumSlices.Reset()
	/*

		sa.FieldTypeSlice.Reset()
		sa.FieldNameSlice.Reset()
		sa.ModelColumnInfo.Reset()
		sa.IntSlice.Reset()
		sa.ByteSlice.Reset()
		sa.TableAliasInJoin = sa.TableAliasInJoin[0:]
		sa.CteCanUsed = sa.CteCanUsed[0:]
		sa.CteBeforeOffset = sa.CteBeforeOffset[0:]
	*/
}

func (sa *SliceAlloctor) InitSliceAlloctor() {
	sa.DatumSlices = &types.DatumSlicePool{}
	sa.DatumSlices.Init()
	/*

			sa.FieldTypeSlice = &types.FieldTypeSliceAllocator{}
			sa.FieldNameSlice = &types.FieldNameSliceAllocator{}
			sa.ModelColumnInfo = &model.ModelColumnInfoSliceAllocator{}
			sa.IntSlice = &IntSliceAllocator{}
			sa.ByteSlice = &ByteSliceAllocator{}
			sa.DatumSlice.InitDatumSlice()
			sa.FieldTypeSlice.InitFieldTypeSlice()
			sa.FieldNameSlice.InitFieldNameSlice()
			sa.ModelColumnInfo.InitColumnInfoSlice()
			sa.IntSlice.InitIntSlice()
			sa.ByteSlice.InitByteSlice()


		sa.TableAliasInJoin = make([]map[string]interface{}, 0)
		sa.CteCanUsed = make([]string, 0)
		sa.CteBeforeOffset = make([]int, 0)
	*/
}

type MapAllocator struct {
	miscMaps          *miscSmallMapPool
	strToDurationMaps *StringToDurationMapPool
}

func (ma *MapAllocator) InitMapAllocator() {
	ma.miscMaps = &miscSmallMapPool{}
	ma.miscMaps.Init()

	ma.strToDurationMaps = &StringToDurationMapPool{}
	ma.strToDurationMaps.Init()
}

func (ma *MapAllocator) Reset() {
	ma.miscMaps.reset()
}

type miscSmallMapPool struct {
	// StmtCtxsmall maps
	statsLoadStatus      map[model.TableItemID]string
	lockTableIDs         map[int64]struct{}
	tblInfo2UnionScan    map[*model.TableInfo]bool
	tableStats           map[int64]interface{}
	isolationReadEngines map[kv.StoreType]struct{}
}

func (m *miscSmallMapPool) Init() {
	m.statsLoadStatus = make(map[model.TableItemID]string, 2)
	m.lockTableIDs = make(map[int64]struct{})
	m.tblInfo2UnionScan = make(map[*model.TableInfo]bool, 2)
	m.tableStats = make(map[int64]interface{})
	m.isolationReadEngines = make(map[kv.StoreType]struct{}, 3)
}

func (m *miscSmallMapPool) getTblInfo2UnionScanMap() map[*model.TableInfo]bool {
	for k := range m.tblInfo2UnionScan {
		delete(m.tblInfo2UnionScan, k)
	}
	return m.tblInfo2UnionScan
}

func (m *miscSmallMapPool) getStatsLoadStatusMap() map[model.TableItemID]string {
	for k := range m.statsLoadStatus {
		delete(m.statsLoadStatus, k)
	}
	return m.statsLoadStatus
}

func (m *miscSmallMapPool) getLockTableIDs() map[int64]struct{} {
	for k := range m.lockTableIDs {
		delete(m.lockTableIDs, k)
	}
	return m.lockTableIDs
}

func (m *miscSmallMapPool) getTableStatsMap() map[int64]interface{} {
	for k := range m.tableStats {
		delete(m.tableStats, k)
	}
	return m.tableStats
}

func (m *miscSmallMapPool) getIsolationReadEnginesMap() map[kv.StoreType]struct{} {
	for k := range m.isolationReadEngines {
		delete(m.isolationReadEngines, k)
	}
	return m.isolationReadEngines
}

func (ma *miscSmallMapPool) reset() {
	// do nothing, when getting map, it will reset the map
}

type StringToDurationMapPool struct {
	mutex sync.Mutex
	maps  [32]map[string]time.Duration
	inUse [32]bool
}

func (m *StringToDurationMapPool) Init() {
	for i := 0; i < 32; i++ {
		m.maps[i] = make(map[string]time.Duration)
	}
}

func (m *StringToDurationMapPool) GetOneMap() map[string]time.Duration {
	var v map[string]time.Duration
	m.mutex.Lock()
	for i := 0; i < 32; i++ {
		if m.inUse[i] {
			continue
		}
		m.inUse[i] = true
		v = m.maps[i]
		break
	}
	m.mutex.Unlock()
	if v == nil {
		return make(map[string]time.Duration)
	}

	for k := range v {
		delete(v, k)
	}
	return v
}

/*

 */
