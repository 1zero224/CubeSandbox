// Copyright (c) 2024 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package sandbox

import "testing"

func TestEndpointTaskAPIEndpoint(t *testing.T) {
	tests := map[string]struct {
		endpoint Endpoint
		want     string
		wantErr  bool
	}{
		"normalizes unix shim address": {
			endpoint: Endpoint{Address: "unix:///run/containerd/cube.sock", Version: 3},
			want:     "ttrpc+unix:///run/containerd/cube.sock",
		},
		"preserves protocol address": {
			endpoint: Endpoint{Address: "ttrpc+unix:///run/containerd/cube.sock", Version: 3},
			want:     "ttrpc+unix:///run/containerd/cube.sock",
		},
		"rejects legacy version": {
			endpoint: Endpoint{Address: "unix:///run/containerd/cube.sock", Version: 2},
			wantErr:  true,
		},
		"rejects unknown transport": {
			endpoint: Endpoint{Address: "tcp://127.0.0.1:1", Version: 3},
			wantErr:  true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := tc.endpoint.TaskAPIEndpoint()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got endpoint %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("TaskAPIEndpoint() error = %v", err)
			}
			if got != tc.want {
				t.Fatalf("TaskAPIEndpoint() = %q, want %q", got, tc.want)
			}
		})
	}
}
