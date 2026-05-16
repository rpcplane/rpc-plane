# Security Policy

## Reporting a vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Report security issues privately via GitHub's advisory system:

**[Report a vulnerability](https://github.com/rpcplane/rpc-plane/security/advisories/new)**

Include:
- A description of the vulnerability and its potential impact
- Steps to reproduce or a proof of concept
- The version of `rpc-plane` affected (`rpc-plane --version`)

You'll receive an acknowledgement within 48 hours. We aim to ship a fix within 14 days for confirmed vulnerabilities, depending on severity and complexity.

## Scope

RPC Plane is a local sidecar proxy. It:

- Handles provider API keys only in memory and in your local config file
- Makes no outbound connections other than to the provider URLs you configure
- Emits no telemetry by default (the `[reporting]` block is opt-in)

The most relevant attack surfaces are:
- Config file parsing (path traversal, env var injection)
- HTTP request handling (request smuggling, header injection)
- Provider response parsing (malformed JSON-RPC)

## Supported versions

Only the latest release receives security fixes. We recommend keeping `rpc-plane` up to date.
