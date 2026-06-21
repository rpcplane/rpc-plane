# RPC Plane — Nomad job (docker driver, TCP)
#
#   nomad job run rpc-plane.nomad.hcl
#
# Registers a `rpc-plane` service in Consul with an HTTP health check, so other
# jobs can discover it (e.g. via a Consul template or `rpc-plane.service.consul`).
# Listen mode: TCP port. For a Unix socket, run it as a task alongside your app
# in the same group with a shared `alloc/` path — see the deployment guide.

job "rpc-plane" {
  datacenters = ["dc1"]
  type        = "service"

  group "rpc-plane" {
    count = 1

    network {
      port "proxy"   { static = 9400 }
      port "metrics" { static = 9401 }
    }

    service {
      name = "rpc-plane"
      port = "proxy"

      check {
        type     = "http"
        path     = "/health"
        interval = "10s"
        timeout  = "2s"
      }
    }

    task "rpc-plane" {
      driver = "docker"

      config {
        image = "ghcr.io/rpcplane/rpc-plane:latest"
        ports = ["proxy", "metrics"]
        args  = ["-c", "/local/rpc-plane.toml", "run"]
      }

      # Provider keys. Prefer Vault in production — see the commented template
      # below. This plaintext env block is for demos only.
      env {
        HELIUS_API_KEY    = "replace-me"
        QUICKNODE_API_KEY = "replace-me"
        TRITON_API_KEY    = "replace-me"
      }

      # The proxy expands ${VAR} from its process env, so the config keeps the
      # ${...} refs literal. In an HCL heredoc, `${` must be escaped as `$${`
      # or Nomad would try to interpolate it.
      template {
        destination = "local/rpc-plane.toml"
        change_mode = "restart"
        data        = <<-EOF
          [server]
          listen         = "0.0.0.0:9400"
          metrics_listen = "0.0.0.0:9401"

          [routing]
          strategy    = "best_score"
          max_retries = 2

          [[providers]]
          name   = "helius"
          url    = "https://mainnet.helius-rpc.com/?api-key=$${HELIUS_API_KEY}"
          weight = 1

          [[providers]]
          name   = "quicknode"
          url    = "https://your-endpoint.quiknode.pro/$${QUICKNODE_API_KEY}"
          weight = 1

          [[providers]]
          name   = "triton"
          url    = "https://your-pool.rpcpool.com/$${TRITON_API_KEY}"
          weight = 1
        EOF
      }

      # Vault alternative — pull keys from a secret and export them as env:
      #
      # vault { policies = ["rpc-plane"] }
      #
      # template {
      #   destination = "secrets/keys.env"
      #   env         = true
      #   data        = <<-EOF
      #     {{ with secret "secret/data/rpc-plane" }}
      #     HELIUS_API_KEY={{ .Data.data.helius_api_key }}
      #     QUICKNODE_API_KEY={{ .Data.data.quicknode_api_key }}
      #     TRITON_API_KEY={{ .Data.data.triton_api_key }}
      #     {{ end }}
      #   EOF
      # }

      resources {
        cpu    = 500   # MHz
        memory = 256   # MB
      }
    }
  }
}
