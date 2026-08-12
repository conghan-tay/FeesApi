package temporalconnection

import "testing"

func TestClientOptions(t *testing.T) {
	tests := []struct {
		name       string
		cfg        Config
		apiKey     string
		wantErr    bool
		wantAuth   bool
		wantTarget string
		wantNS     string
	}{
		{
			name:       "local connection",
			cfg:        Config{Target: "127.0.0.1:7233", Namespace: "default"},
			wantTarget: "127.0.0.1:7233",
			wantNS:     "default",
		},
		{
			name:       "cloud connection",
			cfg:        Config{Target: "fees.example.tmprl.cloud:7233", Namespace: "fees.example", UseAPIKeyAuth: true},
			apiKey:     "test-api-key",
			wantAuth:   true,
			wantTarget: "fees.example.tmprl.cloud:7233",
			wantNS:     "fees.example",
		},
		{
			name:    "missing target",
			cfg:     Config{Namespace: "default"},
			wantErr: true,
		},
		{
			name:    "missing namespace",
			cfg:     Config{Target: "127.0.0.1:7233"},
			wantErr: true,
		},
		{
			name:    "missing cloud API key",
			cfg:     Config{Target: "fees.example.tmprl.cloud:7233", Namespace: "fees.example", UseAPIKeyAuth: true},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options, err := ClientOptions(tt.cfg, tt.apiKey)
			if tt.wantErr {
				if err == nil {
					t.Fatal("ClientOptions returned nil error")
				}
				return
			}
			if err != nil {
				t.Fatalf("ClientOptions returned error: %v", err)
			}
			if options.HostPort != tt.wantTarget || options.Namespace != tt.wantNS {
				t.Fatalf("options = %#v, want target %q namespace %q", options, tt.wantTarget, tt.wantNS)
			}
			if gotAuth := options.Credentials != nil; gotAuth != tt.wantAuth {
				t.Fatalf("credentials present = %t, want %t", gotAuth, tt.wantAuth)
			}
		})
	}
}
