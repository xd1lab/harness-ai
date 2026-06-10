package grpc

// inFlightFor returns the current number of in-flight Run streams the server is
// tracking for tenant. It is a test-only accessor (this file is _test.go) used to
// synchronize tests on the run having registered before issuing Control or
// asserting the per-tenant concurrency cap.
func (s *Server) inFlightFor(tenant string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inflight[tenant]
}
