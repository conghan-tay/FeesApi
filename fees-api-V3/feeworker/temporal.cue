package feeworker

if #Meta.Environment.Cloud == "local" {
	Target:        "127.0.0.1:7233"
	Namespace:     "default"
	UseAPIKeyAuth: false
}

if #Meta.Environment.Cloud != "local" {
	Target:        "fees-api-v3-dev.ebtwx.tmprl.cloud:7233"
	Namespace:     "fees-api-v3-dev.ebtwx"
	UseAPIKeyAuth: true
}
