// SPDX-License-Identifier: Apache-2.0

package main

import (
	igrpc "github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/grpc"
)

// devPrincipalSubject is the synthetic single-tenant subject the dev binary
// injects for every (token-less) request. It is recorded on the principal for
// audit parity with the production edge. The principal_test.go suite pins this
// exact value via its own devSubject constant.
const devPrincipalSubject = "local-dev"

// devAuthConfig returns the single shared [igrpc.AuthConfig] both edges (gRPC and
// REST) of the dev binary use. It runs the dev-insecure path: the interceptor /
// authenticator accept a request with NO bearer token and inject a fixed
// synthetic principal {TenantID: igrpc.DevTenantID, Subject: "local-dev"}.
//
// This is the RLS-bypass fence (K-1/K-2): because OIDC is skipped, dev mode
// injects this synthetic single-tenant principal so igrpc's authorizeTenant still
// runs the SAME code path — single-tenant + loopback-only semantics REPLACE
// multi-tenant RLS rather than deleting the tenant check. It deliberately
// configures NO Keyfunc and NO pinned algorithms; in production that combination
// makes NewAuthenticator/NewAuthInterceptor fail closed, but the DevInsecure flag
// is the explicit, audited opt-in to the loopback dev posture. DevTenantID is a
// valid UUID because the event-store tenant_id contract is a UUID.
func devAuthConfig() igrpc.AuthConfig {
	return igrpc.AuthConfig{
		DevInsecure: true,
		DevPrincipal: igrpc.Principal{
			TenantID: igrpc.DevTenantID,
			Subject:  devPrincipalSubject,
		},
	}
}
