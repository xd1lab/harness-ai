// Command echoserver is a minimal stdio MCP server used by the mcp adapter
// tests to exercise the REAL subprocess path (execSpawn + execTransport)
// without Docker or network. It speaks newline-delimited JSON-RPC 2.0 on
// stdin/stdout and implements just enough of MCP: initialize,
// notifications/initialized (ignored, as notifications carry no ID),
// tools/list, and tools/call for two tools — "echo" (returns its arguments)
// and "env" (returns the process environment, so the confinement test can
// prove the parent's variables never leak into the spawned server).
package main

import (
	"bufio"
	"encoding/json"
	"os"
	"strings"
)

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      *int64          `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type response struct {
	JSONRPC string `json:"jsonrpc"`
	ID      *int64 `json:"id,omitempty"`
	Result  any    `json:"result,omitempty"`
	Error   any    `json:"error,omitempty"`
}

func textResult(text string) any {
	return map[string]any{
		"content": []map[string]any{{"type": "text", "text": text}},
		"isError": false,
	}
}

func main() {
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	out := json.NewEncoder(os.Stdout)

	for sc.Scan() {
		var req request
		if err := json.Unmarshal(sc.Bytes(), &req); err != nil || req.ID == nil {
			continue // notification or noise: no reply
		}
		resp := response{JSONRPC: "2.0", ID: req.ID}
		switch req.Method {
		case "initialize":
			resp.Result = map[string]any{
				"protocolVersion": "2025-06-18",
				"capabilities":    map[string]any{"tools": map[string]any{}},
				"serverInfo":      map[string]any{"name": "echoserver", "version": "1.0.0"},
			}
		case "tools/list":
			resp.Result = map[string]any{"tools": []map[string]any{
				{
					"name":        "echo",
					"description": "echoes its arguments back as JSON",
					"inputSchema": map[string]any{"type": "object"},
				},
				{
					"name":        "env",
					"description": "returns the server process environment",
					"inputSchema": map[string]any{"type": "object"},
				},
			}}
		case "tools/call":
			var p struct {
				Name      string         `json:"name"`
				Arguments map[string]any `json:"arguments"`
			}
			_ = json.Unmarshal(req.Params, &p)
			switch p.Name {
			case "echo":
				args, _ := json.Marshal(p.Arguments)
				resp.Result = textResult("echo:" + string(args))
			case "env":
				resp.Result = textResult(strings.Join(os.Environ(), "\n"))
			default:
				resp.Error = map[string]any{"code": -32602, "message": "unknown tool: " + p.Name}
			}
		default:
			resp.Error = map[string]any{"code": -32601, "message": "method not found: " + req.Method}
		}
		if err := out.Encode(resp); err != nil {
			return
		}
	}
}
