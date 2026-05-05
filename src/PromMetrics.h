//
// This file is part of the aMule Project (ehinny fork).
//
// Prometheus metrics registry. Single global instance, lock-free
// counter/gauge updates from any thread, snapshot text rendering on
// the main thread when /metrics is scraped.
//
// Hot-path discipline: per-event hooks must be O(1), no allocations,
// no locks. Counters use std::atomic<uint64_t>; histograms have
// fixed buckets and atomic increments. The /metrics renderer reads
// the atomics with `memory_order_relaxed` — Prometheus accepts the
// resulting point-in-time approximations because each metric reads
// its own atomic in isolation.
//
// Mutex-contention metrics: a registry of named histograms recorded
// via the MutexLockTimer RAII helper, capturing both wait-for-lock
// time (acquisition latency) and held time (critical-section
// duration). The registry is bounded — entries are pre-declared in
// CPromMetrics::CPromMetrics() so the per-event path stays
// allocation-free.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License as
// published by the Free Software Foundation; either version 2 of
// the License, or (at your option) any later version.
//

#ifndef PROMMETRICS_H
#define PROMMETRICS_H

#include <atomic>
#include <array>
#include <map>
#include <vector>
#include <wx/string.h>
#include <wx/thread.h>

class CPromMetrics {
public:
	CPromMetrics();

	// --- counters (lock-free; safe from any thread) ---
	void IncBytesReceived(uint64_t n)        { m_bytesRx.fetch_add(n, std::memory_order_relaxed); }
	void IncBytesSent(uint64_t n)            { m_bytesTx.fetch_add(n, std::memory_order_relaxed); }
	void IncBlocksReceived()                 { m_blocksRx.fetch_add(1, std::memory_order_relaxed); }
	void IncNoBlockMatch()                   { m_noBlockMatch.fetch_add(1, std::memory_order_relaxed); }
	void IncFlushBuffer()                    { m_flushBuf.fetch_add(1, std::memory_order_relaxed); }
	void IncCorruptedBlocks()                { m_corruptBlocks.fetch_add(1, std::memory_order_relaxed); }
	void IncPacketReceived(uint8_t opcode)   { m_packetByOpcode[opcode].fetch_add(1, std::memory_order_relaxed); }
	void IncSessionTermination()             { m_sessionTerm.fetch_add(1, std::memory_order_relaxed); }
	// upload-side counters
	void IncSendingPartPackets()             { m_sendPartPackets.fetch_add(1, std::memory_order_relaxed); }
	void IncBlockReqReceived()               { m_blockReqRx.fetch_add(1, std::memory_order_relaxed); }
	void IncBlockReqDupDone()                { m_blockReqDupDone.fetch_add(1, std::memory_order_relaxed); }
	void IncBlockReqDupQueued()              { m_blockReqDupQueued.fetch_add(1, std::memory_order_relaxed); }
	void IncSlotGrant()                      { m_slotGrant.fetch_add(1, std::memory_order_relaxed); }
	void IncSlotTermination()                { m_slotTerm.fetch_add(1, std::memory_order_relaxed); }
	void IncFlushBufferTimeBased()           { m_flushBufTime.fetch_add(1, std::memory_order_relaxed); }
	void IncFlushBufferSizeBased()           { m_flushBufSize.fetch_add(1, std::memory_order_relaxed); }
	void IncWriteToBufferCalls()             { m_writeToBuffer.fetch_add(1, std::memory_order_relaxed); }
	// hasher counters
	void IncHashTaskStarted()                { m_hashStarted.fetch_add(1, std::memory_order_relaxed); }
	void IncHashTaskCompleted()              { m_hashCompleted.fetch_add(1, std::memory_order_relaxed); }
	void IncHashTaskFailed()                 { m_hashFailed.fetch_add(1, std::memory_order_relaxed); }
	void AddHashedBytes(uint64_t n)          { m_hashedBytes.fetch_add(n, std::memory_order_relaxed); }
	void IncAichRecovery()                   { m_aichRecov.fetch_add(1, std::memory_order_relaxed); }
	void IncAichVerifyOk()                   { m_aichVerifyOk.fetch_add(1, std::memory_order_relaxed); }
	void IncAichVerifyFail()                 { m_aichVerifyFail.fetch_add(1, std::memory_order_relaxed); }

	// --- gauges (writable from any thread) ---
	void SetActiveHashTasks(uint64_t n)      { m_activeHashTasks.store(n, std::memory_order_relaxed); }
	void IncActiveHashTasks()                { m_activeHashTasks.fetch_add(1, std::memory_order_relaxed); }
	void DecActiveHashTasks()                {
		// floor at 0; relaxed read+CAS would race, but worst case is
		// off-by-one in a transient sample which Prometheus tolerates.
		if (m_activeHashTasks.load(std::memory_order_relaxed) > 0) {
			m_activeHashTasks.fetch_sub(1, std::memory_order_relaxed);
		}
	}

	// --- thread-architecture metrics (master's new I/O threads) ---
	enum ThreadId {
		kThreadWrite = 0,    // CPartFileWriteThread
		kThreadHash,         // CPartFileHashThread
		kThreadUploadDisk,   // CUploadDiskIOThread
		kThreadUploadThr,    // UploadBandwidthThrottler
		kThreadCount
	};
	void IncThreadIteration(ThreadId t)              { m_threadIters[t].fetch_add(1, std::memory_order_relaxed); }
	void AddThreadBusyMicros(ThreadId t, uint64_t u) { m_threadBusyMicros[t].fetch_add(u, std::memory_order_relaxed); }
	void AddThreadIdleMicros(ThreadId t, uint64_t u) { m_threadIdleMicros[t].fetch_add(u, std::memory_order_relaxed); }
	void SetThreadQueueDepth(ThreadId t, uint64_t n) { m_threadQueueDepth[t].store(n, std::memory_order_relaxed); }
	// Bandwidth throttler bucket level (atomic gauge updated by Reserve/Refund).
	void SetDownloadBucketBytes(int64_t b)           { m_downloadBucket.store(b, std::memory_order_relaxed); }
	void IncDownloadThrottlerRefunds()               { m_downloadRefunds.fetch_add(1, std::memory_order_relaxed); }
	void IncDownloadThrottlerStarvations()           { m_downloadStarvations.fetch_add(1, std::memory_order_relaxed); }

	// Hash-quiescent skip counter — incremented every time FlushBuffer
	// Phase 3 declines to enqueue dirty parts because the file is still
	// receiving blocks.  If this is large while pending dirty parts is
	// also large, the hash thread is starved by a continuous download.
	void IncHashSkipQuiescent()                      { m_hashSkipQuiescent.fetch_add(1, std::memory_order_relaxed); }

	// --- histogram observers (fixed buckets) ---
	// Time buckets: 0.001 0.005 0.01 0.05 0.1 0.5 1 5 10 30 (s)
	void ObserveFlushBufferDuration(double seconds)    { Observe(m_hFlush,         seconds); }
	void ObserveOnReceiveDuration(double seconds)      { Observe(m_hOnRecv,        seconds); }
	void ObserveOnReceiveBytes(uint64_t bytes);
	void ObserveBlockCompleteDuration(double seconds)  { Observe(m_hBlockComplete, seconds); }

	// --- mutex contention registry ---
	// Mutexes are pre-registered in the constructor; the hot path
	// (MutexLockTimer dtor) only does an atomic add per bucket. Names
	// must match those declared in CPromMetrics::CPromMetrics().
	// Mutex contention histogram: wait time = blocked waiting to
	// acquire; hold time = ran in critical section.  Both in
	// seconds, but stored as nanoseconds internally (lock waits
	// are typically sub-microsecond when uncontended).
	struct MutexHistogram {
		struct H {
			std::array<std::atomic<uint64_t>, 10> buckets;
			std::atomic<uint64_t> count;
			std::atomic<uint64_t> sumNanos;
			H() {
				for (auto& b : buckets) b.store(0);
				count.store(0);
				sumNanos.store(0);
			}
		};
		H wait;
		H hold;
		std::atomic<uint64_t> contended; // wait > 1us — proxy for "we blocked"
		MutexHistogram() : contended(0) {}
	};
	// Mutex names — extend by adding to ms_mutexNames in the .cpp.
	MutexHistogram* GetMutexHist(const char* name);
	void ObserveMutexWait(MutexHistogram* h, uint64_t nanos);
	void ObserveMutexHold(MutexHistogram* h, uint64_t nanos);

	// --- snapshot, rendered as Prometheus text-format ---
	wxString Render();

private:
	// Top-level dispatch — keep Render() short.
	void RenderTransferCounters(wxString& out);
	void RenderUploadCounters(wxString& out);
	void RenderHasherCounters(wxString& out);
	void RenderHistograms(wxString& out);
	void RenderTransferGauges(wxString& out);
	void RenderThreadGauges(wxString& out);
	void RenderMutexHistograms(wxString& out);
	void RenderProcLinux(wxString& out);

	struct Histogram {
		// 10 buckets.  +Inf bucket = count (Prometheus convention).
		std::array<std::atomic<uint64_t>, 10> buckets;
		std::atomic<uint64_t> count;
		std::atomic<uint64_t> sumMicros; // microseconds for Observe();
		                                 // bytes for ObserveOnReceiveBytes()
		Histogram() {
			for (auto& b : buckets) b.store(0);
			count.store(0);
			sumMicros.store(0);
		}
	};

	void Observe(Histogram& h, double seconds);
	void RenderHistogram(wxString& out, const wxString& name, const Histogram& h);
	void RenderMutexHistogram(wxString& out, const wxString& name, const MutexHistogram::H& h, const char* suffix);

	std::atomic<uint64_t> m_bytesRx{0};
	std::atomic<uint64_t> m_bytesTx{0};
	std::atomic<uint64_t> m_blocksRx{0};
	std::atomic<uint64_t> m_noBlockMatch{0};
	std::atomic<uint64_t> m_flushBuf{0};
	std::atomic<uint64_t> m_corruptBlocks{0};
	std::atomic<uint64_t> m_sessionTerm{0};
	std::array<std::atomic<uint64_t>, 256> m_packetByOpcode;

	// upload-side
	std::atomic<uint64_t> m_sendPartPackets{0};
	std::atomic<uint64_t> m_blockReqRx{0};
	std::atomic<uint64_t> m_blockReqDupDone{0};
	std::atomic<uint64_t> m_blockReqDupQueued{0};
	std::atomic<uint64_t> m_slotGrant{0};
	std::atomic<uint64_t> m_slotTerm{0};
	std::atomic<uint64_t> m_flushBufTime{0};
	std::atomic<uint64_t> m_flushBufSize{0};
	std::atomic<uint64_t> m_writeToBuffer{0};

	// hasher
	std::atomic<uint64_t> m_hashStarted{0};
	std::atomic<uint64_t> m_hashCompleted{0};
	std::atomic<uint64_t> m_hashFailed{0};
	std::atomic<uint64_t> m_hashedBytes{0};
	std::atomic<uint64_t> m_aichRecov{0};
	std::atomic<uint64_t> m_aichVerifyOk{0};
	std::atomic<uint64_t> m_aichVerifyFail{0};
	std::atomic<uint64_t> m_activeHashTasks{0};

	// thread-architecture
	std::array<std::atomic<uint64_t>, kThreadCount> m_threadIters;
	std::array<std::atomic<uint64_t>, kThreadCount> m_threadBusyMicros;
	std::array<std::atomic<uint64_t>, kThreadCount> m_threadIdleMicros;
	std::array<std::atomic<uint64_t>, kThreadCount> m_threadQueueDepth;

	std::atomic<int64_t>  m_downloadBucket{0};
	std::atomic<uint64_t> m_downloadRefunds{0};
	std::atomic<uint64_t> m_downloadStarvations{0};
	std::atomic<uint64_t> m_hashSkipQuiescent{0};

	Histogram m_hFlush;
	Histogram m_hOnRecv;
	Histogram m_hBlockComplete;
	Histogram m_hOnRecvBytes;

	// Mutex registry — fixed entries declared in ctor.
	struct MutexEntry {
		const char* name;
		MutexHistogram h;
	};
	std::vector<MutexEntry*> m_mutexes;
};

extern CPromMetrics g_PromMetrics;

// RAII helper for histogram-of-duration observers. Use as:
//   ScopedHistTimer t(&CPromMetrics::ObserveFlushBufferDuration);
class ScopedHistTimer {
public:
	typedef void (CPromMetrics::*ObsFn)(double);
	explicit ScopedHistTimer(ObsFn fn);
	~ScopedHistTimer();
private:
	ObsFn m_fn;
	uint64_t m_t0Micros;
};

// RAII helper that wraps a wxMutex acquisition with two timers:
// the wait-for-lock latency, and the held-section duration. Both
// reported to a pre-registered MutexHistogram. Use it instead of
// `wxMutexLocker` at a hot site — the cost is two clock_gettime
// calls and one atomic add per bucket update.
class wxMutex;
class MutexLockTimer {
public:
	MutexLockTimer(const char* name, wxMutex& m);
	~MutexLockTimer();
	MutexLockTimer(const MutexLockTimer&) = delete;
	MutexLockTimer& operator=(const MutexLockTimer&) = delete;
private:
	wxMutex* m_m;
	CPromMetrics::MutexHistogram* m_h;
	uint64_t m_t0Hold; // nanoseconds; set after acquire
};

// std::mutex flavour for CPartFile::m_hpartfileMutex.
namespace std { class mutex; }
class StdMutexLockTimer {
public:
	StdMutexLockTimer(const char* name, std::mutex& m);
	~StdMutexLockTimer();
	StdMutexLockTimer(const StdMutexLockTimer&) = delete;
	StdMutexLockTimer& operator=(const StdMutexLockTimer&) = delete;
private:
	std::mutex* m_m;
	CPromMetrics::MutexHistogram* m_h;
	uint64_t m_t0Hold;
};

#endif // PROMMETRICS_H
