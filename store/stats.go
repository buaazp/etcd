// Copyright 2015 CoreOS, Inc.
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

package store

import (
	"encoding/json"
	"sync/atomic"
)

const (
	SetSuccess = iota
	SetFail
	DeleteSuccess
	DeleteFail
	CreateSuccess
	CreateFail
	UpdateSuccess
	UpdateFail
	CompareAndSwapSuccess
	CompareAndSwapFail
	GetSuccess
	GetFail
	ExpireCount
	CompareAndDeleteSuccess
	CompareAndDeleteFail
	AddSuccess
	AddFail
	PushSuccess
	PushFail
	PopSuccess
	PopFail
	ConfirmSuccess
	ConfirmFail
	RemoveSuccess
	RemoveFail
)

type Stats struct {

	// Number of get requests
	GetSuccess uint64 `json:"getsSuccess"`
	GetFail    uint64 `json:"getsFail"`

	// Number of sets requests
	SetSuccess uint64 `json:"setsSuccess"`
	SetFail    uint64 `json:"setsFail"`

	// Number of delete requests
	DeleteSuccess uint64 `json:"deleteSuccess"`
	DeleteFail    uint64 `json:"deleteFail"`

	// Number of update requests
	UpdateSuccess uint64 `json:"updateSuccess"`
	UpdateFail    uint64 `json:"updateFail"`

	// Number of create requests
	CreateSuccess uint64 `json:"createSuccess"`
	CreateFail    uint64 `json:"createFail"`

	// Number of testAndSet requests
	CompareAndSwapSuccess uint64 `json:"compareAndSwapSuccess"`
	CompareAndSwapFail    uint64 `json:"compareAndSwapFail"`

	// Number of compareAndDelete requests
	CompareAndDeleteSuccess uint64 `json:"compareAndDeleteSuccess"`
	CompareAndDeleteFail    uint64 `json:"compareAndDeleteFail"`

	ExpireCount uint64 `json:"expireCount"`

	Watchers uint64 `json:"watchers"`

	AddSuccess    uint64 `json:"addSuccess"`
	AddFail       uint64 `json:"addFail"`
	PushSuccess   uint64 `json:"pushSuccess"`
	PushFail      uint64 `json:"pushFail"`
	PopSuccess    uint64 `json:"popSuccess"`
	PopFail       uint64 `json:"popFail"`
	ConfirmSucces uint64 `json:"confirmSucces"`
	ConfirmFail   uint64 `json:"confirmFail"`
	RemoveSucces  uint64 `json:"removeSucces"`
	RemoveFail    uint64 `json:"removeFail"`
}

func newStats() *Stats {
	s := new(Stats)
	return s
}

func (s *Stats) clone() *Stats {
	return &Stats{
		GetSuccess:              s.GetSuccess,
		GetFail:                 s.GetFail,
		SetSuccess:              s.SetSuccess,
		SetFail:                 s.SetFail,
		DeleteSuccess:           s.DeleteSuccess,
		DeleteFail:              s.DeleteFail,
		UpdateSuccess:           s.UpdateSuccess,
		UpdateFail:              s.UpdateFail,
		CreateSuccess:           s.CreateSuccess,
		CreateFail:              s.CreateFail,
		CompareAndSwapSuccess:   s.CompareAndSwapSuccess,
		CompareAndSwapFail:      s.CompareAndSwapFail,
		CompareAndDeleteSuccess: s.CompareAndDeleteSuccess,
		CompareAndDeleteFail:    s.CompareAndDeleteFail,
		ExpireCount:             s.ExpireCount,
		Watchers:                s.Watchers,
		AddSuccess:              s.AddSuccess,
		AddFail:                 s.AddFail,
		PushSuccess:             s.PushSuccess,
		PushFail:                s.PushFail,
		PopSuccess:              s.PopSuccess,
		PopFail:                 s.PopFail,
		ConfirmSucces:           s.ConfirmSucces,
		ConfirmFail:             s.ConfirmFail,
		RemoveSucces:            s.RemoveSucces,
		RemoveFail:              s.RemoveFail,
	}
}

func (s *Stats) toJson() []byte {
	b, _ := json.Marshal(s)
	return b
}

func (s *Stats) Inc(field int) {
	switch field {
	case SetSuccess:
		atomic.AddUint64(&s.SetSuccess, 1)
	case SetFail:
		atomic.AddUint64(&s.SetFail, 1)
	case CreateSuccess:
		atomic.AddUint64(&s.CreateSuccess, 1)
	case CreateFail:
		atomic.AddUint64(&s.CreateFail, 1)
	case DeleteSuccess:
		atomic.AddUint64(&s.DeleteSuccess, 1)
	case DeleteFail:
		atomic.AddUint64(&s.DeleteFail, 1)
	case GetSuccess:
		atomic.AddUint64(&s.GetSuccess, 1)
	case GetFail:
		atomic.AddUint64(&s.GetFail, 1)
	case UpdateSuccess:
		atomic.AddUint64(&s.UpdateSuccess, 1)
	case UpdateFail:
		atomic.AddUint64(&s.UpdateFail, 1)
	case CompareAndSwapSuccess:
		atomic.AddUint64(&s.CompareAndSwapSuccess, 1)
	case CompareAndSwapFail:
		atomic.AddUint64(&s.CompareAndSwapFail, 1)
	case CompareAndDeleteSuccess:
		atomic.AddUint64(&s.CompareAndDeleteSuccess, 1)
	case CompareAndDeleteFail:
		atomic.AddUint64(&s.CompareAndDeleteFail, 1)
	case ExpireCount:
		atomic.AddUint64(&s.ExpireCount, 1)
	case AddSuccess:
		atomic.AddUint64(&s.AddSuccess, 1)
	case AddFail:
		atomic.AddUint64(&s.AddFail, 1)
	case PushSuccess:
		atomic.AddUint64(&s.PushSuccess, 1)
	case PushFail:
		atomic.AddUint64(&s.PushFail, 1)
	case PopSuccess:
		atomic.AddUint64(&s.PopSuccess, 1)
	case PopFail:
		atomic.AddUint64(&s.PopFail, 1)
	case ConfirmSuccess:
		atomic.AddUint64(&s.ConfirmSucces, 1)
	case ConfirmFail:
		atomic.AddUint64(&s.ConfirmFail, 1)
	case RemoveSuccess:
		atomic.AddUint64(&s.RemoveSucces, 1)
	case RemoveFail:
		atomic.AddUint64(&s.RemoveFail, 1)
	}
}
