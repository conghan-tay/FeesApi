package fees

if #Meta.Environment.Cloud == "local" {
	Target:        "127.0.0.1:7233"
	Namespace:     "default"
	UseAPIKeyAuth: false
}

if #Meta.Environment.Cloud != "local" {
	Target:        "TEMPORAL_CLOUD_ENDPOINT"
	Namespace:     "TEMPORAL_CLOUD_NAMESPACE"
	UseAPIKeyAuth: true
}
