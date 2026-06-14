// SPDX-License-Identifier: Apache-2.0

package main

// T-4 (AC-7) — RED. A no-token request reaching EITHER the gRPC server or the REST
// facade must carry the synthetic dev principal {TenantID: DevTenantID,
// Subject:"local-dev"} (injected via AuthConfig{DevInsecure:true,...}), and
// authorizeTenant must derive the tenant from the VERIFIED PRINCIPAL, not the
// request body. Single-tenant + loopback REPLACES multi-tenant RLS along the SAME
// code path. It references devAuthConfig(), the dev binary's single shared
// AuthConfig, which does not exist yet → RED.

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc"

	igrpc "github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/grpc"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// devSubject is the synthetic single-tenant subject the dev binary injects.
const devSubject = "local-dev"

// TestDevPrincipal_GRPCInterceptor_InjectsSyntheticTenant asserts AC-7 (a): a
// no-token unary call passing through the dev gRPC auth interceptor lands on a
// handler context carrying {DevTenantID, "local-dev"}.
func TestDevPrincipal_GRPCInterceptor_InjectsSyntheticTenant(t *testing.T) {
	cfg := devAuthConfig() // does not exist yet (RED)

	interceptor, err := igrpc.NewAuthInterceptor(cfg)
	require.NoError(t, err)

	var gotPrincipal igrpc.Principal
	var gotOK bool
	handler := func(ctx context.Context, _ any) (any, error) {
		gotPrincipal, gotOK = igrpc.PrincipalFromContext(ctx)
		return nil, nil
	}

	// No incoming metadata at all (a no-token request): the dev path must accept it
	// and inject the synthetic principal.
	_, err = interceptor(context.Background(), struct{}{}, &grpc.UnaryServerInfo{}, handler)
	require.NoError(t, err)

	require.True(t, gotOK, "dev interceptor must place a verified principal on the context")
	assert.Equal(t, igrpc.DevTenantID, gotPrincipal.TenantID, "tenant must be the synthetic dev tenant")
	assert.Equal(t, devSubject, gotPrincipal.Subject, "subject must be the synthetic local-dev subject")
}

// TestDevPrincipal_RESTPath_InjectsSyntheticTenant asserts AC-7 (b): the REST
// facade's auth path (VerifyBearer → ContextWithPrincipal, exactly what
// rest.withAuth runs) injects the same synthetic principal for a no-Authorization
// request. We drive the shared Authenticator the REST handler is constructed with.
func TestDevPrincipal_RESTPath_InjectsSyntheticTenant(t *testing.T) {
	cfg := devAuthConfig() // RED

	auth, err := igrpc.NewAuthenticator(cfg)
	require.NoError(t, err)

	// rest.withAuth calls VerifyBearer("") when the Authorization header is absent;
	// the dev path returns the synthetic principal regardless of the (empty) token.
	p, err := auth.VerifyBearer("")
	require.NoError(t, err)

	ctx := igrpc.ContextWithPrincipal(context.Background(), p)
	got, ok := igrpc.PrincipalFromContext(ctx)
	require.True(t, ok, "REST path must place a verified principal on the context")
	assert.Equal(t, igrpc.DevTenantID, got.TenantID)
	assert.Equal(t, devSubject, got.Subject)
}

// TestDevTenantID_IsValidUUID asserts AC-7 (c): DevTenantID parses as a valid UUID
// (the event-store tenant_id column is a UUID; a non-UUID dev tenant breaks the
// first CreateSession). This guards the dev binary's choice of synthetic tenant.
func TestDevTenantID_IsValidUUID(t *testing.T) {
	_, err := uuid.Parse(igrpc.DevTenantID)
	assert.NoError(t, err, "DevTenantID must be a valid UUID")
}

// TestDevAuthConfig_IsDevInsecureSingleTenant pins the dev binary's auth posture:
// the shared AuthConfig is dev-insecure (no Keyfunc, no pinned algs) and names the
// synthetic single-tenant principal. This is the config consumed by BOTH edges.
func TestDevAuthConfig_IsDevInsecureSingleTenant(t *testing.T) {
	cfg := devAuthConfig() // RED
	assert.True(t, cfg.DevInsecure, "dev auth must run the dev-insecure path on both edges")
	assert.Equal(t, igrpc.DevTenantID, cfg.DevPrincipal.TenantID)
	assert.Equal(t, devSubject, cfg.DevPrincipal.Subject)
	assert.Nil(t, cfg.Keyfunc, "dev mode must not configure a production Keyfunc")
}
