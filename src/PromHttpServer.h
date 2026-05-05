//
// This file is part of the aMule Project (ehinny fork).
//
// Minimal HTTP listener serving GET /metrics with the Prometheus
// text-format body produced by g_PromMetrics.Render().
//

#ifndef PROMHTTPSERVER_H
#define PROMHTTPSERVER_H

#include <stdint.h>

class CPromListener;

class CPromHttpServer {
public:
	CPromHttpServer();
	~CPromHttpServer();

	// Bind to `port` on 0.0.0.0. Port 0 disables (returns true).
	// Returns false on bind failure.
	bool Start(uint16_t port);
	void Stop();

private:
	CPromListener* m_listener;
};

#endif // PROMHTTPSERVER_H
