//
// This file is part of the aMule Project (ehinny fork).
//
// HTTP /metrics listener — wxSocketServer-based. Accepts a
// connection, reads up to 4 KiB of request, replies with the
// Prometheus text-format body or 404, then closes. No keep-alive.
//
// Built directly on wxSocket (not the CLibSocket wrapper) because
// we want NOWAIT + a real timeout — CLibSocket's WaitForRead is a
// no-op under ASIO_SOCKETS, which would deadlock our handler when
// the client (Prometheus) sends headers and then waits silently
// for the response.
//

#include "PromHttpServer.h"
#include "PromMetrics.h"

#include "Logger.h"
#include "amuleIPV4Address.h"
#include <common/Format.h>

#include <wx/event.h>
#include <wx/socket.h>
#include <wx/string.h>
#include <wx/utils.h>
#include <cstring>

#ifdef __linux__
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#endif

namespace {

void ServeOne(wxSocketBase* sock) {
	if (!sock) return;

	// Block for at most 2 s waiting for request bytes; the entire
	// scrape should finish in milliseconds on a LAN, anything
	// longer is a hung client and we just cut it off.
	sock->SetFlags(wxSOCKET_WAITALL_READ | wxSOCKET_BLOCK);
	sock->SetTimeout(2);

	const size_t kReqMax = 4096;
	uint8_t buf[kReqMax];
	uint32_t got = 0;
	for (int spin = 0; spin < 32 && got < kReqMax; ++spin) {
		// WaitForRead with short timeout so we never hang forever.
		if (!sock->WaitForRead(0, 200)) break;
		sock->Read(buf + got, 1);
		const wxUint32 r = sock->LastCount();
		if (r == 0) break;
		got += r;
		if (got >= 4 && memcmp(buf + got - 4, "\r\n\r\n", 4) == 0) break;
		if (got >= 2 && memcmp(buf + got - 2, "\n\n", 2) == 0) break;
	}

	wxString reqLine;
	for (uint32_t i = 0; i < got; ++i) {
		if (buf[i] == '\r' || buf[i] == '\n') break;
		reqLine.Append((wxChar)buf[i]);
	}

	wxString status;
	wxString body;
	if (reqLine.StartsWith(wxT("GET /metrics"))) {
		status = wxT("200 OK");
		body   = g_PromMetrics.Render();
	} else {
		status = wxT("404 Not Found");
		body   = wxT("Not Found\n");
	}

	const wxScopedCharBuffer bodyUtf8 = body.ToUTF8();
	wxString header = CFormat(wxT(
		"HTTP/1.1 %s\r\n"
		"Content-Type: text/plain; version=0.0.4\r\n"
		"Content-Length: %u\r\n"
		"Connection: close\r\n"
		"\r\n"))
		% status
		% (unsigned)bodyUtf8.length();

	sock->SetFlags(wxSOCKET_WAITALL_WRITE | wxSOCKET_BLOCK);
	sock->SetTimeout(10);

	auto writeAll = [&](const char* data, size_t len) -> bool {
		size_t off = 0;
		while (off < len) {
			sock->Write(data + off, len - off);
			const wxUint32 w = sock->LastCount();
			if (w == 0) return false;
			off += w;
			if (sock->Error() && !sock->WaitForWrite(2, 0)) return false;
		}
		return true;
	};

	const wxScopedCharBuffer headerUtf8 = header.ToUTF8();
	if (writeAll(headerUtf8.data(), headerUtf8.length()) && bodyUtf8.length() > 0) {
		writeAll(bodyUtf8.data(), bodyUtf8.length());
	}

	// Linger close: read until peer FIN (or 100 ms) so the kernel has
	// time to drain our write queue. wxSocket's Close() does an
	// abortive close otherwise, which RSTs and truncates the body
	// for large /metrics payloads.
	sock->SetFlags(wxSOCKET_NONE);
	sock->SetTimeout(1);
	{
		uint8_t scratch[512];
		for (int i = 0; i < 5; ++i) {
			if (!sock->WaitForRead(0, 100)) break;
			sock->Read(scratch, sizeof(scratch));
			if (sock->LastCount() == 0) break;
		}
	}
	sock->Close();
}

} // namespace

// Tiny wxSocketServer subclass that fires an event on every accept.
class CPromListener : public wxEvtHandler {
public:
	CPromListener() : m_server(NULL) {}

	bool Bind(uint16_t port) {
		wxIPV4address addr;
		addr.AnyAddress();
		addr.Service(port);
		m_server = new wxSocketServer(addr, wxSOCKET_REUSEADDR);
		if (!m_server->IsOk()) {
			delete m_server;
			m_server = NULL;
			return false;
		}
		m_server->SetEventHandler(*this, /*id=*/0);
		m_server->SetNotify(wxSOCKET_CONNECTION_FLAG);
		m_server->Notify(true);
		Connect(0, wxEVT_SOCKET, wxSocketEventHandler(CPromListener::OnAccept));
		return true;
	}

	void Stop() {
		if (m_server) {
			m_server->Close();
			m_server->Destroy();
			m_server = NULL;
		}
	}

private:
	void OnAccept(wxSocketEvent& WXUNUSED(event)) {
		while (true) {
			wxSocketBase* s = m_server->Accept(/*wait=*/false);
			if (!s) break;
			ServeOne(s);
			s->Destroy();
		}
	}

	wxSocketServer* m_server;
};

CPromHttpServer::CPromHttpServer() : m_listener(NULL) {}

CPromHttpServer::~CPromHttpServer() { Stop(); }

bool CPromHttpServer::Start(uint16_t port) {
	if (port == 0) {
		AddLogLineN(wxT("PromHttpServer: PrometheusPort=0, exporter disabled."));
		return true;
	}
	if (m_listener) return true;

	m_listener = new CPromListener();
	if (!m_listener->Bind(port)) {
		AddLogLineC(CFormat(wxT("PromHttpServer: bind failed on port %u")) % port);
		delete m_listener;
		m_listener = NULL;
		return false;
	}
	AddLogLineN(CFormat(wxT("PromHttpServer: /metrics listening on 0.0.0.0:%u")) % port);
	return true;
}

void CPromHttpServer::Stop() {
	if (m_listener) {
		m_listener->Stop();
		delete m_listener;
		m_listener = NULL;
	}
}
