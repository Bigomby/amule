//
// This file is part of the aMule Project (ehinny fork).
//
// Implementation of the Prometheus metrics registry.
//

#include "PromMetrics.h"

#include "amule.h"
#include "ClientList.h"
#include "DownloadQueue.h"
#include "PartFile.h"
#include "SharedFileList.h"
#include "Statistics.h"
#include "UploadQueue.h"
#include "updownclient.h"

#include <wx/datetime.h>
#include <wx/string.h>
#include <wx/thread.h>
#include <common/Format.h>

#include <mutex>

#ifdef __linux__
#include <sys/ioctl.h>
#include <linux/sockios.h> // SIOCOUTQ, SIOCINQ
#include <sys/resource.h>  // getrusage
#include <sys/sysinfo.h>   // sysinfo (memory)
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <cstring>
#endif

#include <fstream>
#include <sstream>
#include <ctime>
#include <time.h>

CPromMetrics g_PromMetrics;

namespace {
// Time-second buckets for transfer/op histograms.
const double kBucketBounds[10] = {
	0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0
};
// Byte-size buckets for OnReceive payload histogram.
const uint64_t kByteBucketBounds[10] = {
	128, 512, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216
};
// Mutex wait/hold buckets — sub-microsecond floor because most
// uncontended acquisitions are <100 ns; long tail goes up to 100 ms
// to catch I/O-on-lock pathologies.
//   100ns  500ns  1us  5us  50us  500us  5ms  50ms  500ms  5s
const double kMutexBucketBoundsNs[10] = {
	1e2,    5e2,   1e3, 5e3, 5e4,  5e5,   5e6, 5e7,  5e8,   5e9
};

uint64_t NowMicros() {
	wxLongLong now = wxGetUTCTimeMillis();
	return static_cast<uint64_t>(now.GetValue()) * 1000ULL;
}

uint64_t NowNanos() {
#ifdef CLOCK_MONOTONIC
	struct timespec ts;
	if (clock_gettime(CLOCK_MONOTONIC, &ts) == 0) {
		return static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL
		     + static_cast<uint64_t>(ts.tv_nsec);
	}
#endif
	return NowMicros() * 1000ULL;
}

// epoch seconds the daemon started — used for amule_uptime_seconds.
double g_StartEpochSeconds = 0.0;

// Mutex registry — populated once in CPromMetrics ctor. Pointers
// stay stable for the process lifetime so the hot path can cache
// them in static locals.
const char* const kMutexNames[] = {
	"partfile_write_thread",        // CPartFileWriteThread::m_mutex
	"partfile_hash_thread",         // CPartFileHashThread::m_mutex
	"upload_disk_io_thread",        // CUploadDiskIOThread::m_mutex
	"upload_throttler_send",        // UploadBandwidthThrottler::m_sendLocker
	"upload_throttler_temp",        // UploadBandwidthThrottler::m_tempQueueLocker
	"upload_throttler_newdata",     // UploadBandwidthThrottler::m_newDataMutex
	"partfile_hpartfile",           // CPartFile::m_hpartfileMutex (std::mutex)
	"upload_queue_uploading_list",  // CUploadQueue::m_uploadingListMutex
	NULL,
};

#ifdef __linux__
// Read a line-keyed value from a /proc text file.
wxString ProcKey(const char* path, const char* key) {
	std::ifstream f(path);
	if (!f.is_open()) return wxEmptyString;
	std::string line;
	const size_t klen = strlen(key);
	while (std::getline(f, line)) {
		if (line.compare(0, klen, key) == 0 && line[klen] == ':') {
			size_t pos = klen + 1;
			while (pos < line.size() && (line[pos] == ' ' || line[pos] == '\t')) pos++;
			return wxString::FromUTF8(line.substr(pos).c_str());
		}
	}
	return wxEmptyString;
}

uint64_t ProcKeyKbToBytes(const char* path, const char* key) {
	wxString v = ProcKey(path, key);
	if (v.IsEmpty()) return 0;
	unsigned long long n = 0;
	std::sscanf(v.utf8_str().data(), "%llu", &n);
	return static_cast<uint64_t>(n) * 1024ULL;
}

struct ProcStat {
	uint64_t utime, stime;
	uint64_t num_threads;
	uint64_t starttime;
	uint64_t vsize;
	int64_t  rss_pages;
};
bool ReadProcStat(ProcStat& out) {
	std::ifstream f("/proc/self/stat");
	if (!f.is_open()) return false;
	std::string s((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
	auto rp = s.rfind(')');
	if (rp == std::string::npos) return false;
	std::istringstream iss(s.substr(rp + 2));
	std::string state;
	uint64_t ppid, pgrp, session, tty_nr, tpgid, flags, minflt, cminflt, majflt, cmajflt;
	int64_t  cutime, cstime, priority, nice, itrealvalue;
	iss >> state >> ppid >> pgrp >> session >> tty_nr >> tpgid >> flags
	    >> minflt >> cminflt >> majflt >> cmajflt
	    >> out.utime >> out.stime >> cutime >> cstime
	    >> priority >> nice >> out.num_threads >> itrealvalue
	    >> out.starttime >> out.vsize >> out.rss_pages;
	return true;
}

struct ProcIo {
	uint64_t rchar, wchar;
	uint64_t read_bytes, write_bytes;
};
bool ReadProcIo(ProcIo& out) {
	std::memset(&out, 0, sizeof(out));
	std::ifstream f("/proc/self/io");
	if (!f.is_open()) return false;
	std::string line;
	while (std::getline(f, line)) {
		auto colon = line.find(':');
		if (colon == std::string::npos) continue;
		std::string key = line.substr(0, colon);
		uint64_t v = 0;
		std::sscanf(line.c_str() + colon + 1, " %llu", reinterpret_cast<unsigned long long*>(&v));
		if (key == "rchar")        out.rchar = v;
		else if (key == "wchar")   out.wchar = v;
		else if (key == "read_bytes")  out.read_bytes = v;
		else if (key == "write_bytes") out.write_bytes = v;
	}
	return true;
}

struct ProcNet { uint64_t rx_bytes, tx_bytes, rx_packets, tx_packets; };
bool ReadProcNet(ProcNet& out) {
	std::memset(&out, 0, sizeof(out));
	std::ifstream f("/proc/self/net/dev");
	if (!f.is_open()) return false;
	std::string line;
	int linenum = 0;
	while (std::getline(f, line)) {
		linenum++;
		if (linenum <= 2) continue;
		auto colon = line.find(':');
		if (colon == std::string::npos) continue;
		std::string iface = line.substr(0, colon);
		while (!iface.empty() && iface.front() == ' ') iface.erase(0, 1);
		if (iface == "lo") continue;
		uint64_t rxb, rxp, dummy[6], txb, txp;
		int matched = std::sscanf(line.c_str() + colon + 1,
			" %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu",
			(unsigned long long*)&rxb, (unsigned long long*)&rxp,
			(unsigned long long*)&dummy[0], (unsigned long long*)&dummy[1],
			(unsigned long long*)&dummy[2], (unsigned long long*)&dummy[3],
			(unsigned long long*)&dummy[4], (unsigned long long*)&dummy[5],
			(unsigned long long*)&txb, (unsigned long long*)&txp);
		if (matched >= 10) {
			out.rx_bytes   += rxb;
			out.tx_bytes   += txb;
			out.rx_packets += rxp;
			out.tx_packets += txp;
		}
	}
	return true;
}
#endif // __linux__
} // namespace

CPromMetrics::CPromMetrics() {
	for (auto& a : m_packetByOpcode) a.store(0);
	for (auto& a : m_threadIters) a.store(0);
	for (auto& a : m_threadBusyMicros) a.store(0);
	for (auto& a : m_threadIdleMicros) a.store(0);
	for (auto& a : m_threadQueueDepth) a.store(0);
	g_StartEpochSeconds = static_cast<double>(::time(NULL));

	for (size_t i = 0; kMutexNames[i] != NULL; ++i) {
		MutexEntry* e = new MutexEntry;
		e->name = kMutexNames[i];
		m_mutexes.push_back(e);
	}
}

CPromMetrics::MutexHistogram* CPromMetrics::GetMutexHist(const char* name) {
	// Linear scan — registry is small (<16 entries) and called once
	// per mutex site (cached in a static local).
	for (auto* e : m_mutexes) {
		if (std::strcmp(e->name, name) == 0) return &e->h;
	}
	return NULL;
}

void CPromMetrics::ObserveMutexWait(MutexHistogram* h, uint64_t nanos) {
	if (!h) return;
	h->wait.sumNanos.fetch_add(nanos, std::memory_order_relaxed);
	h->wait.count.fetch_add(1, std::memory_order_relaxed);
	if (nanos > 1000) h->contended.fetch_add(1, std::memory_order_relaxed);
	const double ns = static_cast<double>(nanos);
	for (size_t i = 0; i < 10; ++i) {
		if (ns <= kMutexBucketBoundsNs[i]) {
			h->wait.buckets[i].fetch_add(1, std::memory_order_relaxed);
		}
	}
}

void CPromMetrics::ObserveMutexHold(MutexHistogram* h, uint64_t nanos) {
	if (!h) return;
	h->hold.sumNanos.fetch_add(nanos, std::memory_order_relaxed);
	h->hold.count.fetch_add(1, std::memory_order_relaxed);
	const double ns = static_cast<double>(nanos);
	for (size_t i = 0; i < 10; ++i) {
		if (ns <= kMutexBucketBoundsNs[i]) {
			h->hold.buckets[i].fetch_add(1, std::memory_order_relaxed);
		}
	}
}

void CPromMetrics::Observe(Histogram& h, double seconds) {
	uint64_t micros = static_cast<uint64_t>(seconds * 1e6);
	h.sumMicros.fetch_add(micros, std::memory_order_relaxed);
	h.count.fetch_add(1, std::memory_order_relaxed);
	for (size_t i = 0; i < 10; ++i) {
		if (seconds <= kBucketBounds[i]) {
			h.buckets[i].fetch_add(1, std::memory_order_relaxed);
		}
	}
}

void CPromMetrics::ObserveOnReceiveBytes(uint64_t bytes) {
	m_hOnRecvBytes.sumMicros.fetch_add(bytes, std::memory_order_relaxed);
	m_hOnRecvBytes.count.fetch_add(1, std::memory_order_relaxed);
	for (size_t i = 0; i < 10; ++i) {
		if (bytes <= kByteBucketBounds[i]) {
			m_hOnRecvBytes.buckets[i].fetch_add(1, std::memory_order_relaxed);
		}
	}
}

void CPromMetrics::RenderHistogram(wxString& out, const wxString& name, const Histogram& h) {
	const uint64_t count = h.count.load(std::memory_order_relaxed);
	uint64_t cumulative = 0;
	for (size_t i = 0; i < 10; ++i) {
		cumulative = h.buckets[i].load(std::memory_order_relaxed);
		out << CFormat(wxT("%s_bucket{le=\"%g\"} %llu\n"))
			% name % kBucketBounds[i] % (unsigned long long)cumulative;
	}
	out << CFormat(wxT("%s_bucket{le=\"+Inf\"} %llu\n"))
		% name % (unsigned long long)count;
	const double sumSec = h.sumMicros.load(std::memory_order_relaxed) / 1e6;
	out << CFormat(wxT("%s_sum %.6f\n")) % name % sumSec;
	out << CFormat(wxT("%s_count %llu\n")) % name % (unsigned long long)count;
}

void CPromMetrics::RenderMutexHistogram(wxString& out, const wxString& name,
                                        const MutexHistogram::H& h, const char* /*suffix*/) {
	const uint64_t count = h.count.load(std::memory_order_relaxed);
	for (size_t i = 0; i < 10; ++i) {
		uint64_t v = h.buckets[i].load(std::memory_order_relaxed);
		// Convert ns bucket boundary to seconds for Prometheus convention.
		const double leSec = kMutexBucketBoundsNs[i] / 1e9;
		out << CFormat(wxT("%s_bucket{le=\"%g\"} %llu\n"))
			% name % leSec % (unsigned long long)v;
	}
	out << CFormat(wxT("%s_bucket{le=\"+Inf\"} %llu\n"))
		% name % (unsigned long long)count;
	const double sumSec = h.sumNanos.load(std::memory_order_relaxed) / 1e9;
	out << CFormat(wxT("%s_sum %.9f\n")) % name % sumSec;
	out << CFormat(wxT("%s_count %llu\n")) % name % (unsigned long long)count;
}

// === Render dispatch =======================================================

wxString CPromMetrics::Render() {
	wxString out;
	out.Alloc(16384);
	RenderTransferCounters(out);
	RenderUploadCounters(out);
	RenderHasherCounters(out);
	RenderHistograms(out);
	RenderTransferGauges(out);
	RenderThreadGauges(out);
	RenderMutexHistograms(out);
#ifdef __linux__
	RenderProcLinux(out);
#endif
	return out;
}

void CPromMetrics::RenderTransferCounters(wxString& out) {
	out << wxT("# HELP amule_bytes_received_total Bytes received from peers (download payload).\n");
	out << wxT("# TYPE amule_bytes_received_total counter\n");
	out << CFormat(wxT("amule_bytes_received_total %llu\n"))
		% (unsigned long long)m_bytesRx.load(std::memory_order_relaxed);

	out << wxT("# HELP amule_bytes_sent_total Bytes sent to peers (upload payload).\n");
	out << wxT("# TYPE amule_bytes_sent_total counter\n");
	out << CFormat(wxT("amule_bytes_sent_total %llu\n"))
		% (unsigned long long)m_bytesTx.load(std::memory_order_relaxed);

	out << wxT("# HELP amule_blocks_received_total Blocks completed and removed from the pending list.\n");
	out << wxT("# TYPE amule_blocks_received_total counter\n");
	out << CFormat(wxT("amule_blocks_received_total %llu\n"))
		% (unsigned long long)m_blocksRx.load(std::memory_order_relaxed);

	out << wxT("# HELP amule_no_block_match_total ProcessBlockPacket arrivals discarded for not matching any pending range.\n");
	out << wxT("# TYPE amule_no_block_match_total counter\n");
	out << CFormat(wxT("amule_no_block_match_total %llu\n"))
		% (unsigned long long)m_noBlockMatch.load(std::memory_order_relaxed);

	out << wxT("# HELP amule_flushbuffer_total CPartFile::FlushBuffer invocations.\n");
	out << wxT("# TYPE amule_flushbuffer_total counter\n");
	out << CFormat(wxT("amule_flushbuffer_total %llu\n"))
		% (unsigned long long)m_flushBuf.load(std::memory_order_relaxed);

	out << wxT("# HELP amule_corrupted_blocks_total ICH/AICH-detected corrupted blocks.\n");
	out << wxT("# TYPE amule_corrupted_blocks_total counter\n");
	out << CFormat(wxT("amule_corrupted_blocks_total %llu\n"))
		% (unsigned long long)m_corruptBlocks.load(std::memory_order_relaxed);

	out << wxT("# HELP amule_session_terminations_total Upload/download sessions closed cleanly or via DOWNLOADTIMEOUT.\n");
	out << wxT("# TYPE amule_session_terminations_total counter\n");
	out << CFormat(wxT("amule_session_terminations_total %llu\n"))
		% (unsigned long long)m_sessionTerm.load(std::memory_order_relaxed);

	out << wxT("# HELP amule_packets_received_total ed2k packets parsed by CEMSocket per opcode.\n");
	out << wxT("# TYPE amule_packets_received_total counter\n");
	for (size_t op = 0; op < 256; ++op) {
		uint64_t v = m_packetByOpcode[op].load(std::memory_order_relaxed);
		if (v == 0) continue;
		out << CFormat(wxT("amule_packets_received_total{opcode=\"0x%02x\"} %llu\n"))
			% (unsigned)op % (unsigned long long)v;
	}
}

void CPromMetrics::RenderUploadCounters(wxString& out) {
	out << wxT("# HELP amule_sendingpart_packets_total OP_SENDINGPART(_I64) packets emitted.\n");
	out << wxT("# TYPE amule_sendingpart_packets_total counter\n");
	out << CFormat(wxT("amule_sendingpart_packets_total %llu\n"))
		% (unsigned long long)m_sendPartPackets.load(std::memory_order_relaxed);

	out << wxT("# HELP amule_block_request_received_total Range entries the peer asked us to upload (post-AddReqBlock).\n");
	out << wxT("# TYPE amule_block_request_received_total counter\n");
	out << CFormat(wxT("amule_block_request_received_total %llu\n"))
		% (unsigned long long)m_blockReqRx.load(std::memory_order_relaxed);

	out << wxT("# HELP amule_block_request_dup_done_total REQUESTPARTS ranges dropped because we already finished serving them.\n");
	out << wxT("# TYPE amule_block_request_dup_done_total counter\n");
	out << CFormat(wxT("amule_block_request_dup_done_total %llu\n"))
		% (unsigned long long)m_blockReqDupDone.load(std::memory_order_relaxed);

	out << wxT("# HELP amule_block_request_dup_queued_total REQUESTPARTS ranges dropped because they were already queued.\n");
	out << wxT("# TYPE amule_block_request_dup_queued_total counter\n");
	out << CFormat(wxT("amule_block_request_dup_queued_total %llu\n"))
		% (unsigned long long)m_blockReqDupQueued.load(std::memory_order_relaxed);

	out << wxT("# HELP amule_slot_grants_total Upload slots granted via OP_ACCEPTUPLOADREQ.\n");
	out << wxT("# TYPE amule_slot_grants_total counter\n");
	out << CFormat(wxT("amule_slot_grants_total %llu\n"))
		% (unsigned long long)m_slotGrant.load(std::memory_order_relaxed);

	out << wxT("# HELP amule_slot_terminations_total Upload slots torn down.\n");
	out << wxT("# TYPE amule_slot_terminations_total counter\n");
	out << CFormat(wxT("amule_slot_terminations_total %llu\n"))
		% (unsigned long long)m_slotTerm.load(std::memory_order_relaxed);

	out << wxT("# HELP amule_flushbuffer_time_based_total FlushBuffer triggered because BUFFER_TIME_LIMIT elapsed.\n");
	out << wxT("# TYPE amule_flushbuffer_time_based_total counter\n");
	out << CFormat(wxT("amule_flushbuffer_time_based_total %llu\n"))
		% (unsigned long long)m_flushBufTime.load(std::memory_order_relaxed);

	out << wxT("# HELP amule_flushbuffer_size_based_total FlushBuffer triggered because m_nTotalBufferData exceeded GetFileBufferSize().\n");
	out << wxT("# TYPE amule_flushbuffer_size_based_total counter\n");
	out << CFormat(wxT("amule_flushbuffer_size_based_total %llu\n"))
		% (unsigned long long)m_flushBufSize.load(std::memory_order_relaxed);

	out << wxT("# HELP amule_writetobuffer_calls_total CPartFile::WriteToBuffer invocations.\n");
	out << wxT("# TYPE amule_writetobuffer_calls_total counter\n");
	out << CFormat(wxT("amule_writetobuffer_calls_total %llu\n"))
		% (unsigned long long)m_writeToBuffer.load(std::memory_order_relaxed);
}

void CPromMetrics::RenderHasherCounters(wxString& out) {
	out << wxT("# HELP amule_hash_tasks_started_total Hashing tasks dispatched.\n");
	out << wxT("# TYPE amule_hash_tasks_started_total counter\n");
	out << CFormat(wxT("amule_hash_tasks_started_total %llu\n"))
		% (unsigned long long)m_hashStarted.load(std::memory_order_relaxed);

	out << wxT("# HELP amule_hash_tasks_completed_total Hashing tasks that finished successfully.\n");
	out << wxT("# TYPE amule_hash_tasks_completed_total counter\n");
	out << CFormat(wxT("amule_hash_tasks_completed_total %llu\n"))
		% (unsigned long long)m_hashCompleted.load(std::memory_order_relaxed);

	out << wxT("# HELP amule_hash_tasks_failed_total Hashing tasks that aborted.\n");
	out << wxT("# TYPE amule_hash_tasks_failed_total counter\n");
	out << CFormat(wxT("amule_hash_tasks_failed_total %llu\n"))
		% (unsigned long long)m_hashFailed.load(std::memory_order_relaxed);

	out << wxT("# HELP amule_hashed_bytes_total Total bytes consumed by the hasher.\n");
	out << wxT("# TYPE amule_hashed_bytes_total counter\n");
	out << CFormat(wxT("amule_hashed_bytes_total %llu\n"))
		% (unsigned long long)m_hashedBytes.load(std::memory_order_relaxed);

	out << wxT("# HELP amule_aich_recoveries_total AICH chunk recoveries triggered.\n");
	out << wxT("# TYPE amule_aich_recoveries_total counter\n");
	out << CFormat(wxT("amule_aich_recoveries_total %llu\n"))
		% (unsigned long long)m_aichRecov.load(std::memory_order_relaxed);

	out << wxT("# HELP amule_aich_verify_ok_total AICH part verifications that passed.\n");
	out << wxT("# TYPE amule_aich_verify_ok_total counter\n");
	out << CFormat(wxT("amule_aich_verify_ok_total %llu\n"))
		% (unsigned long long)m_aichVerifyOk.load(std::memory_order_relaxed);

	out << wxT("# HELP amule_aich_verify_fail_total AICH part verifications that failed.\n");
	out << wxT("# TYPE amule_aich_verify_fail_total counter\n");
	out << CFormat(wxT("amule_aich_verify_fail_total %llu\n"))
		% (unsigned long long)m_aichVerifyFail.load(std::memory_order_relaxed);

	out << wxT("# HELP amule_hash_tasks_active Hashing tasks currently running.\n");
	out << wxT("# TYPE amule_hash_tasks_active gauge\n");
	out << CFormat(wxT("amule_hash_tasks_active %llu\n"))
		% (unsigned long long)m_activeHashTasks.load(std::memory_order_relaxed);

	const uint64_t started   = m_hashStarted.load(std::memory_order_relaxed);
	const uint64_t completed = m_hashCompleted.load(std::memory_order_relaxed);
	const uint64_t failed    = m_hashFailed.load(std::memory_order_relaxed);
	uint64_t pending = (started > completed + failed) ? (started - completed - failed) : 0;
	out << wxT("# HELP amule_hash_tasks_pending Files queued but not yet finished or failed.\n");
	out << wxT("# TYPE amule_hash_tasks_pending gauge\n");
	out << CFormat(wxT("amule_hash_tasks_pending %llu\n"))
		% (unsigned long long)pending;
}

void CPromMetrics::RenderHistograms(wxString& out) {
	out << wxT("# HELP amule_flushbuffer_duration_seconds Time spent inside CPartFile::FlushBuffer.\n");
	out << wxT("# TYPE amule_flushbuffer_duration_seconds histogram\n");
	RenderHistogram(out, wxT("amule_flushbuffer_duration_seconds"), m_hFlush);

	out << wxT("# HELP amule_onreceive_duration_seconds Time spent inside CEMSocket::OnReceive.\n");
	out << wxT("# TYPE amule_onreceive_duration_seconds histogram\n");
	RenderHistogram(out, wxT("amule_onreceive_duration_seconds"), m_hOnRecv);

	out << wxT("# HELP amule_block_complete_duration_seconds Time from REQUESTPARTS send to last sub-packet of the block.\n");
	out << wxT("# TYPE amule_block_complete_duration_seconds histogram\n");
	RenderHistogram(out, wxT("amule_block_complete_duration_seconds"), m_hBlockComplete);

	const uint64_t cnt = m_hOnRecvBytes.count.load(std::memory_order_relaxed);
	out << wxT("# HELP amule_onreceive_bytes Bytes read in a single OnReceive invocation.\n");
	out << wxT("# TYPE amule_onreceive_bytes histogram\n");
	for (size_t i = 0; i < 10; ++i) {
		out << CFormat(wxT("amule_onreceive_bytes_bucket{le=\"%llu\"} %llu\n"))
			% (unsigned long long)kByteBucketBounds[i]
			% (unsigned long long)m_hOnRecvBytes.buckets[i].load(std::memory_order_relaxed);
	}
	out << CFormat(wxT("amule_onreceive_bytes_bucket{le=\"+Inf\"} %llu\n")) % (unsigned long long)cnt;
	out << CFormat(wxT("amule_onreceive_bytes_sum %llu\n"))
		% (unsigned long long)m_hOnRecvBytes.sumMicros.load(std::memory_order_relaxed);
	out << CFormat(wxT("amule_onreceive_bytes_count %llu\n")) % (unsigned long long)cnt;
}

void CPromMetrics::RenderTransferGauges(wxString& out) {
	if (theApp && theApp->downloadqueue) {
		std::vector<CPartFile*> files;
		theApp->downloadqueue->CopyFileList(files, false);
		uint32 activeDownloads = 0;
		uint32 activeSources = 0;

		out << wxT("# HELP amule_buffered_data_bytes Bytes pending in CPartFile::m_BufferedData_list.\n");
		out << wxT("# TYPE amule_buffered_data_bytes gauge\n");
		out << wxT("# HELP amule_buffered_data_items List item count of CPartFile::m_BufferedData_list.\n");
		out << wxT("# TYPE amule_buffered_data_items gauge\n");

		for (auto pf : files) {
			if (!pf) continue;
			wxString hash = pf->GetFileHash().Encode();
			out << CFormat(wxT("amule_buffered_data_bytes{file_hash=\"%s\"} %u\n"))
				% hash % (unsigned)pf->GetBufferedDataSize();
			out << CFormat(wxT("amule_buffered_data_items{file_hash=\"%s\"} %u\n"))
				% hash % (unsigned)pf->GetBufferedDataCount();
			if (pf->GetTransferingSrcCount() > 0) ++activeDownloads;
			activeSources += pf->GetSourceCount();
		}

		out << wxT("# HELP amule_active_downloads PartFiles currently transferring.\n");
		out << wxT("# TYPE amule_active_downloads gauge\n");
		out << CFormat(wxT("amule_active_downloads %u\n")) % activeDownloads;

		out << wxT("# HELP amule_active_sources Sum of GetSourceCount across queue.\n");
		out << wxT("# TYPE amule_active_sources gauge\n");
		out << CFormat(wxT("amule_active_sources %u\n")) % activeSources;
	}

	out << wxT("# HELP amule_download_rate_bytes_per_sec Aggregate download rate.\n");
	out << wxT("# TYPE amule_download_rate_bytes_per_sec gauge\n");
	out << CFormat(wxT("amule_download_rate_bytes_per_sec %.0f\n"))
		% (double)theStats::GetDownloadRate();

	out << wxT("# HELP amule_upload_rate_bytes_per_sec Aggregate upload rate.\n");
	out << wxT("# TYPE amule_upload_rate_bytes_per_sec gauge\n");
	out << CFormat(wxT("amule_upload_rate_bytes_per_sec %.0f\n"))
		% (double)theStats::GetUploadRate();

	if (theApp && theApp->uploadqueue) {
		out << wxT("# HELP amule_upload_slots_used Active upload slots.\n");
		out << wxT("# TYPE amule_upload_slots_used gauge\n");
		out << CFormat(wxT("amule_upload_slots_used %u\n"))
			% (unsigned)theApp->uploadqueue->GetUploadingList().size();

		out << wxT("# HELP amule_upload_waiting_clients Clients on the wait queue.\n");
		out << wxT("# TYPE amule_upload_waiting_clients gauge\n");
		out << CFormat(wxT("amule_upload_waiting_clients %u\n"))
			% (unsigned)theApp->uploadqueue->GetWaitingList().size();
	}

	if (theApp && theApp->sharedfiles) {
		out << wxT("# HELP amule_shared_files Files currently published as shared.\n");
		out << wxT("# TYPE amule_shared_files gauge\n");
		out << CFormat(wxT("amule_shared_files %u\n"))
			% (unsigned)theApp->sharedfiles->GetCount();
	}
}

void CPromMetrics::RenderThreadGauges(wxString& out) {
	const char* names[kThreadCount] = {
		"partfile_write", "partfile_hash", "upload_disk_io", "upload_throttler"
	};

	out << wxT("# HELP amule_thread_iterations_total Worker thread loop iterations (one per dequeue or wakeup).\n");
	out << wxT("# TYPE amule_thread_iterations_total counter\n");
	for (size_t i = 0; i < kThreadCount; ++i) {
		out << CFormat(wxT("amule_thread_iterations_total{thread=\"%s\"} %llu\n"))
			% wxString::FromAscii(names[i])
			% (unsigned long long)m_threadIters[i].load(std::memory_order_relaxed);
	}

	out << wxT("# HELP amule_thread_busy_seconds_total Time the worker thread spent processing dequeued work.\n");
	out << wxT("# TYPE amule_thread_busy_seconds_total counter\n");
	for (size_t i = 0; i < kThreadCount; ++i) {
		const double sec = m_threadBusyMicros[i].load(std::memory_order_relaxed) / 1e6;
		out << CFormat(wxT("amule_thread_busy_seconds_total{thread=\"%s\"} %.6f\n"))
			% wxString::FromAscii(names[i]) % sec;
	}

	out << wxT("# HELP amule_thread_idle_seconds_total Time the worker thread spent waiting on its condition.\n");
	out << wxT("# TYPE amule_thread_idle_seconds_total counter\n");
	for (size_t i = 0; i < kThreadCount; ++i) {
		const double sec = m_threadIdleMicros[i].load(std::memory_order_relaxed) / 1e6;
		out << CFormat(wxT("amule_thread_idle_seconds_total{thread=\"%s\"} %.6f\n"))
			% wxString::FromAscii(names[i]) % sec;
	}

	out << wxT("# HELP amule_thread_queue_depth Items pending in the worker thread's input queue.\n");
	out << wxT("# TYPE amule_thread_queue_depth gauge\n");
	for (size_t i = 0; i < kThreadCount; ++i) {
		out << CFormat(wxT("amule_thread_queue_depth{thread=\"%s\"} %llu\n"))
			% wxString::FromAscii(names[i])
			% (unsigned long long)m_threadQueueDepth[i].load(std::memory_order_relaxed);
	}

	out << wxT("# HELP amule_download_throttler_bucket_bytes Current capacity left in the global download token bucket.\n");
	out << wxT("# TYPE amule_download_throttler_bucket_bytes gauge\n");
	out << CFormat(wxT("amule_download_throttler_bucket_bytes %lld\n"))
		% (long long)m_downloadBucket.load(std::memory_order_relaxed);

	out << wxT("# HELP amule_download_throttler_refunds_total Reserve-then-refund operations (TCP partial reads).\n");
	out << wxT("# TYPE amule_download_throttler_refunds_total counter\n");
	out << CFormat(wxT("amule_download_throttler_refunds_total %llu\n"))
		% (unsigned long long)m_downloadRefunds.load(std::memory_order_relaxed);

	out << wxT("# HELP amule_download_throttler_starvations_total OnReceive calls that found the bucket empty.\n");
	out << wxT("# TYPE amule_download_throttler_starvations_total counter\n");
	out << CFormat(wxT("amule_download_throttler_starvations_total %llu\n"))
		% (unsigned long long)m_downloadStarvations.load(std::memory_order_relaxed);
}

void CPromMetrics::RenderMutexHistograms(wxString& out) {
	out << wxT("# HELP amule_mutex_wait_seconds Time blocked waiting to acquire a named mutex.\n");
	out << wxT("# TYPE amule_mutex_wait_seconds histogram\n");
	for (auto* e : m_mutexes) {
		const wxString name = wxString::FromAscii(e->name);
		// One histogram per mutex; Prometheus wants the histogram's
		// label structure repeated, so emit name="X" labels.
		const MutexHistogram::H& h = e->h.wait;
		const uint64_t count = h.count.load(std::memory_order_relaxed);
		for (size_t i = 0; i < 10; ++i) {
			uint64_t v = h.buckets[i].load(std::memory_order_relaxed);
			const double leSec = kMutexBucketBoundsNs[i] / 1e9;
			out << CFormat(wxT("amule_mutex_wait_seconds_bucket{mutex=\"%s\",le=\"%g\"} %llu\n"))
				% name % leSec % (unsigned long long)v;
		}
		out << CFormat(wxT("amule_mutex_wait_seconds_bucket{mutex=\"%s\",le=\"+Inf\"} %llu\n"))
			% name % (unsigned long long)count;
		out << CFormat(wxT("amule_mutex_wait_seconds_sum{mutex=\"%s\"} %.9f\n"))
			% name % (h.sumNanos.load(std::memory_order_relaxed) / 1e9);
		out << CFormat(wxT("amule_mutex_wait_seconds_count{mutex=\"%s\"} %llu\n"))
			% name % (unsigned long long)count;
	}

	out << wxT("# HELP amule_mutex_hold_seconds Time held inside the critical section of a named mutex.\n");
	out << wxT("# TYPE amule_mutex_hold_seconds histogram\n");
	for (auto* e : m_mutexes) {
		const wxString name = wxString::FromAscii(e->name);
		const MutexHistogram::H& h = e->h.hold;
		const uint64_t count = h.count.load(std::memory_order_relaxed);
		for (size_t i = 0; i < 10; ++i) {
			uint64_t v = h.buckets[i].load(std::memory_order_relaxed);
			const double leSec = kMutexBucketBoundsNs[i] / 1e9;
			out << CFormat(wxT("amule_mutex_hold_seconds_bucket{mutex=\"%s\",le=\"%g\"} %llu\n"))
				% name % leSec % (unsigned long long)v;
		}
		out << CFormat(wxT("amule_mutex_hold_seconds_bucket{mutex=\"%s\",le=\"+Inf\"} %llu\n"))
			% name % (unsigned long long)count;
		out << CFormat(wxT("amule_mutex_hold_seconds_sum{mutex=\"%s\"} %.9f\n"))
			% name % (h.sumNanos.load(std::memory_order_relaxed) / 1e9);
		out << CFormat(wxT("amule_mutex_hold_seconds_count{mutex=\"%s\"} %llu\n"))
			% name % (unsigned long long)count;
	}

	out << wxT("# HELP amule_mutex_contended_total Acquisitions where wait > 1 microsecond (proxy for contended).\n");
	out << wxT("# TYPE amule_mutex_contended_total counter\n");
	for (auto* e : m_mutexes) {
		out << CFormat(wxT("amule_mutex_contended_total{mutex=\"%s\"} %llu\n"))
			% wxString::FromAscii(e->name)
			% (unsigned long long)e->h.contended.load(std::memory_order_relaxed);
	}
}

#ifdef __linux__
void CPromMetrics::RenderProcLinux(wxString& out) {
	out << wxT("# HELP amule_uptime_seconds Seconds since the daemon started.\n");
	out << wxT("# TYPE amule_uptime_seconds gauge\n");
	out << CFormat(wxT("amule_uptime_seconds %.0f\n"))
		% (static_cast<double>(::time(NULL)) - g_StartEpochSeconds);

	out << wxT("# HELP amule_start_time_seconds Unix timestamp at which the daemon started.\n");
	out << wxT("# TYPE amule_start_time_seconds gauge\n");
	out << CFormat(wxT("amule_start_time_seconds %.0f\n")) % g_StartEpochSeconds;

	{
		ProcStat ps;
		if (ReadProcStat(ps)) {
			const long ticksPerSec = sysconf(_SC_CLK_TCK);
			const long pageSize    = sysconf(_SC_PAGESIZE);
			const double utimeSec = ticksPerSec > 0 ? (double)ps.utime / ticksPerSec : 0.0;
			const double stimeSec = ticksPerSec > 0 ? (double)ps.stime / ticksPerSec : 0.0;

			out << wxT("# HELP amule_process_cpu_user_seconds_total CPU seconds in user mode.\n");
			out << wxT("# TYPE amule_process_cpu_user_seconds_total counter\n");
			out << CFormat(wxT("amule_process_cpu_user_seconds_total %.3f\n")) % utimeSec;

			out << wxT("# HELP amule_process_cpu_system_seconds_total CPU seconds in kernel mode.\n");
			out << wxT("# TYPE amule_process_cpu_system_seconds_total counter\n");
			out << CFormat(wxT("amule_process_cpu_system_seconds_total %.3f\n")) % stimeSec;

			out << wxT("# HELP amule_process_cpu_seconds_total CPU seconds total.\n");
			out << wxT("# TYPE amule_process_cpu_seconds_total counter\n");
			out << CFormat(wxT("amule_process_cpu_seconds_total %.3f\n")) % (utimeSec + stimeSec);

			out << wxT("# HELP amule_process_threads OS thread count.\n");
			out << wxT("# TYPE amule_process_threads gauge\n");
			out << CFormat(wxT("amule_process_threads %llu\n")) % (unsigned long long)ps.num_threads;

			out << wxT("# HELP amule_process_virtual_memory_bytes VSize.\n");
			out << wxT("# TYPE amule_process_virtual_memory_bytes gauge\n");
			out << CFormat(wxT("amule_process_virtual_memory_bytes %llu\n")) % (unsigned long long)ps.vsize;

			out << wxT("# HELP amule_process_resident_memory_bytes RSS.\n");
			out << wxT("# TYPE amule_process_resident_memory_bytes gauge\n");
			out << CFormat(wxT("amule_process_resident_memory_bytes %lld\n"))
				% (long long)(ps.rss_pages * pageSize);
		}
	}

	{
		const uint64_t vmPeak = ProcKeyKbToBytes("/proc/self/status", "VmPeak");
		const uint64_t vmHwm  = ProcKeyKbToBytes("/proc/self/status", "VmHWM");
		const uint64_t vmData = ProcKeyKbToBytes("/proc/self/status", "VmData");
		const uint64_t vmSwap = ProcKeyKbToBytes("/proc/self/status", "VmSwap");
		out << wxT("# HELP amule_process_virtual_memory_peak_bytes High-water-mark of VSize.\n");
		out << wxT("# TYPE amule_process_virtual_memory_peak_bytes gauge\n");
		out << CFormat(wxT("amule_process_virtual_memory_peak_bytes %llu\n")) % (unsigned long long)vmPeak;
		out << wxT("# HELP amule_process_resident_memory_peak_bytes High-water-mark of RSS.\n");
		out << wxT("# TYPE amule_process_resident_memory_peak_bytes gauge\n");
		out << CFormat(wxT("amule_process_resident_memory_peak_bytes %llu\n")) % (unsigned long long)vmHwm;
		out << wxT("# HELP amule_process_data_memory_bytes Heap + writable static data.\n");
		out << wxT("# TYPE amule_process_data_memory_bytes gauge\n");
		out << CFormat(wxT("amule_process_data_memory_bytes %llu\n")) % (unsigned long long)vmData;
		out << wxT("# HELP amule_process_swap_memory_bytes Anon pages swapped out.\n");
		out << wxT("# TYPE amule_process_swap_memory_bytes gauge\n");
		out << CFormat(wxT("amule_process_swap_memory_bytes %llu\n")) % (unsigned long long)vmSwap;
	}

	{
		uint64_t fdCount = 0;
		DIR* d = opendir("/proc/self/fd");
		if (d) {
			while (struct dirent* e = readdir(d)) {
				if (e->d_name[0] != '.') fdCount++;
			}
			closedir(d);
		}
		out << wxT("# HELP amule_process_open_fds Open file descriptors.\n");
		out << wxT("# TYPE amule_process_open_fds gauge\n");
		out << CFormat(wxT("amule_process_open_fds %llu\n")) % (unsigned long long)fdCount;

		struct rlimit rl;
		if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
			out << wxT("# HELP amule_process_max_fds Soft RLIMIT_NOFILE.\n");
			out << wxT("# TYPE amule_process_max_fds gauge\n");
			out << CFormat(wxT("amule_process_max_fds %llu\n")) % (unsigned long long)rl.rlim_cur;
		}
	}

	{
		ProcIo io;
		if (ReadProcIo(io)) {
			out << wxT("# HELP amule_process_io_read_bytes_total Bytes via read()/pread() syscalls.\n");
			out << wxT("# TYPE amule_process_io_read_bytes_total counter\n");
			out << CFormat(wxT("amule_process_io_read_bytes_total %llu\n")) % (unsigned long long)io.rchar;
			out << wxT("# HELP amule_process_io_write_bytes_total Bytes via write()/pwrite() syscalls.\n");
			out << wxT("# TYPE amule_process_io_write_bytes_total counter\n");
			out << CFormat(wxT("amule_process_io_write_bytes_total %llu\n")) % (unsigned long long)io.wchar;
			out << wxT("# HELP amule_process_io_disk_read_bytes_total Bytes fetched from storage.\n");
			out << wxT("# TYPE amule_process_io_disk_read_bytes_total counter\n");
			out << CFormat(wxT("amule_process_io_disk_read_bytes_total %llu\n")) % (unsigned long long)io.read_bytes;
			out << wxT("# HELP amule_process_io_disk_write_bytes_total Bytes sent to storage.\n");
			out << wxT("# TYPE amule_process_io_disk_write_bytes_total counter\n");
			out << CFormat(wxT("amule_process_io_disk_write_bytes_total %llu\n")) % (unsigned long long)io.write_bytes;
		}
	}

	{
		ProcNet n;
		if (ReadProcNet(n)) {
			out << wxT("# HELP amule_process_network_receive_bytes_total Bytes received (no lo).\n");
			out << wxT("# TYPE amule_process_network_receive_bytes_total counter\n");
			out << CFormat(wxT("amule_process_network_receive_bytes_total %llu\n")) % (unsigned long long)n.rx_bytes;
			out << wxT("# HELP amule_process_network_transmit_bytes_total Bytes transmitted (no lo).\n");
			out << wxT("# TYPE amule_process_network_transmit_bytes_total counter\n");
			out << CFormat(wxT("amule_process_network_transmit_bytes_total %llu\n")) % (unsigned long long)n.tx_bytes;
			out << wxT("# HELP amule_process_network_receive_packets_total Packets received (no lo).\n");
			out << wxT("# TYPE amule_process_network_receive_packets_total counter\n");
			out << CFormat(wxT("amule_process_network_receive_packets_total %llu\n")) % (unsigned long long)n.rx_packets;
			out << wxT("# HELP amule_process_network_transmit_packets_total Packets transmitted (no lo).\n");
			out << wxT("# TYPE amule_process_network_transmit_packets_total counter\n");
			out << CFormat(wxT("amule_process_network_transmit_packets_total %llu\n")) % (unsigned long long)n.tx_packets;
		}
	}

	{
		struct rusage ru;
		if (getrusage(RUSAGE_SELF, &ru) == 0) {
			out << wxT("# HELP amule_process_minor_faults_total Page faults serviced without I/O.\n");
			out << wxT("# TYPE amule_process_minor_faults_total counter\n");
			out << CFormat(wxT("amule_process_minor_faults_total %lld\n")) % (long long)ru.ru_minflt;
			out << wxT("# HELP amule_process_major_faults_total Page faults that required disk I/O.\n");
			out << wxT("# TYPE amule_process_major_faults_total counter\n");
			out << CFormat(wxT("amule_process_major_faults_total %lld\n")) % (long long)ru.ru_majflt;
			out << wxT("# HELP amule_process_voluntary_ctxt_switches_total Voluntary context switches.\n");
			out << wxT("# TYPE amule_process_voluntary_ctxt_switches_total counter\n");
			out << CFormat(wxT("amule_process_voluntary_ctxt_switches_total %lld\n")) % (long long)ru.ru_nvcsw;
			out << wxT("# HELP amule_process_involuntary_ctxt_switches_total Involuntary context switches.\n");
			out << wxT("# TYPE amule_process_involuntary_ctxt_switches_total counter\n");
			out << CFormat(wxT("amule_process_involuntary_ctxt_switches_total %lld\n")) % (long long)ru.ru_nivcsw;
		}
	}
}
#endif // __linux__

// === RAII helpers ==========================================================

ScopedHistTimer::ScopedHistTimer(ObsFn fn)
	: m_fn(fn)
	, m_t0Micros(NowMicros())
{}

ScopedHistTimer::~ScopedHistTimer() {
	uint64_t dt = NowMicros() - m_t0Micros;
	double sec = dt / 1e6;
	(g_PromMetrics.*m_fn)(sec);
}

MutexLockTimer::MutexLockTimer(const char* name, wxMutex& m)
	: m_m(&m)
	, m_h(g_PromMetrics.GetMutexHist(name))
{
	const uint64_t t0 = NowNanos();
	m_m->Lock();
	const uint64_t t1 = NowNanos();
	g_PromMetrics.ObserveMutexWait(m_h, t1 - t0);
	m_t0Hold = t1;
}

MutexLockTimer::~MutexLockTimer() {
	const uint64_t held = NowNanos() - m_t0Hold;
	m_m->Unlock();
	g_PromMetrics.ObserveMutexHold(m_h, held);
}

StdMutexLockTimer::StdMutexLockTimer(const char* name, std::mutex& m)
	: m_m(&m)
	, m_h(g_PromMetrics.GetMutexHist(name))
{
	const uint64_t t0 = NowNanos();
	m_m->lock();
	const uint64_t t1 = NowNanos();
	g_PromMetrics.ObserveMutexWait(m_h, t1 - t0);
	m_t0Hold = t1;
}

StdMutexLockTimer::~StdMutexLockTimer() {
	const uint64_t held = NowNanos() - m_t0Hold;
	m_m->unlock();
	g_PromMetrics.ObserveMutexHold(m_h, held);
}
