package sectorstorage

import (
	"sync"

	sealtasks "github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

func (a *activeResources) withResources(id WorkerID, wr storiface.WorkerResources, r Resources, locker sync.Locker, cb func() error) error {
	for !a.canHandleRequest(r, id, "withResources", wr) {
		if a.cond == nil {
			a.cond = sync.NewCond(locker)
		}
		a.cond.Wait()
	}

	a.add(wr, r)

	err := cb()

	a.free(wr, r)
	if a.cond != nil {
		a.cond.Broadcast()
	}

	return err
}

// Modified by long 20210318
func (a *activeResources) add(wr storiface.WorkerResources, r Resources) {
	if r.CanGPU {
		a.gpuUsedNum++
		a.gpuUsed = true
	}

	switch r.taskType {
	case sealtasks.TTAddPiece:
		a.apParallelNum++
	case sealtasks.TTPreCommit1:
		a.p1ParallelNum++
	case sealtasks.TTPreCommit2:
		a.p2ParallelNum++
	}

	a.cpuUse += r.Threads(wr.CPUs)
	a.memUsedMin += r.MinMemory
	a.memUsedMax += r.MaxMemory
}

// Modified by long 20210318
func (a *activeResources) free(wr storiface.WorkerResources, r Resources) {
	if r.CanGPU {
		a.gpuUsedNum--
		if a.gpuUsedNum == 0 {
			a.gpuUsed = false
		}
	}

	switch r.taskType {
	case sealtasks.TTAddPiece:
		a.apParallelNum--
	case sealtasks.TTPreCommit1:
		a.p1ParallelNum--
	case sealtasks.TTPreCommit2:
		a.p2ParallelNum--
	}

	a.cpuUse -= r.Threads(wr.CPUs)
	a.memUsedMin -= r.MinMemory
	a.memUsedMax -= r.MaxMemory
}

func (a *activeResources) canHandleRequest(needRes Resources, wid WorkerID, caller string, res storiface.WorkerResources) bool {

	// TODO: dedupe needRes.BaseMinMemory per task type (don't add if that task is already running)
	minNeedMem := res.MemReserved + a.memUsedMin + needRes.MinMemory + needRes.BaseMinMemory
	if minNeedMem > res.MemPhysical {
		log.Debugf("sched: not scheduling on worker %s for %s; not enough physical memory - need: %dM, have %dM", wid, caller, minNeedMem/mib, res.MemPhysical/mib)
		return false
	}

	maxNeedMem := res.MemReserved + a.memUsedMax + needRes.MaxMemory + needRes.BaseMinMemory

	if maxNeedMem > res.MemSwap+res.MemPhysical {
		log.Debugf("sched: not scheduling on worker %s for %s; not enough virtual memory - need: %dM, have %dM", wid, caller, maxNeedMem/mib, (res.MemSwap+res.MemPhysical)/mib)
		return false
	}

	if a.cpuUse+needRes.Threads(res.CPUs) > res.CPUs {
		log.Debugf("sched: not scheduling on worker %s for %s; not enough threads, need %d, %d in use, target %d", wid, caller, needRes.Threads(res.CPUs), a.cpuUse, res.CPUs)
		return false
	}

	// Deleted by long 20210510
	// if len(res.GPUs) > 0 && needRes.CanGPU { // Meanless
	// 	if a.gpuUsed {
	// 		log.Debugf("sched[C2]: not scheduling on worker %s for %s; GPU in use", wid, caller)
	// 		return false
	// 	}
	// }

	// Added by long 20210405 -------------------------------------------------
	switch needRes.taskType {
	case sealtasks.TTAddPiece:
		// if a.apParallelNum > 0 || a.p2ParallelNum > 0 || a.p1ParallelNum >= LO_P1_PARALLEL_NUM {
		if a.apParallelNum > 0 || a.p1ParallelNum >= LO_P1_PARALLEL_NUM {
			// 1. AP and P2 are mutually exclusive, and only one AP is allowed to be runnig in parallel.
			// 2. When the worker was filled by P1, there is no need to get AP.
			log.Debugf("sched[AP]: not scheduling on worker %s for %s;", wid, caller)
			return false
		}

	case sealtasks.TTPreCommit1:
		if a.p1ParallelNum >= LO_P1_PARALLEL_NUM {
			log.Debugf("sched[P1]: not scheduling on worker %s for %s; P1ParallelNum get max", wid, caller)
			return false
		}

	// case sealtasks.TTPreCommit2:
	// 	if a.apParallelNum > 0 {
	// 		log.Debugf("sched[P2]: not scheduling on worker %s for %s; AP is running...", wid, caller)
	// 		return false
	// 	}
	}
	// ------------------------------------------------------------------------

	return true
}

func (a *activeResources) utilization(wr storiface.WorkerResources) float64 {
	var max float64

	cpu := float64(a.cpuUse) / float64(wr.CPUs)
	max = cpu

	memMin := float64(a.memUsedMin+wr.MemReserved) / float64(wr.MemPhysical)
	if memMin > max {
		max = memMin
	}

	memMax := float64(a.memUsedMax+wr.MemReserved) / float64(wr.MemPhysical+wr.MemSwap)
	if memMax > max {
		max = memMax
	}

	return max
}

func (wh *workerHandle) utilization() float64 {
	wh.lk.Lock()
	u := wh.active.utilization(wh.info.Resources)
	u += wh.preparing.utilization(wh.info.Resources)
	wh.lk.Unlock()
	wh.wndLk.Lock()
	for _, window := range wh.activeWindows {
		u += window.allocated.utilization(wh.info.Resources)
	}
	wh.wndLk.Unlock()

	return u
}
