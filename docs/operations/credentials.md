# Credential Operations

M3 uses bearer tokens with a deliberately small credential model.

## Operator credential

Production server config points to:

~~~toml
operator_token_file = "/etc/lmt/operator.token"
~~~

The file should normally be root:lmt mode 0640.

The operator token is a root/admin credential. M3 does not implement operator roles or multiple identities.

Rotate it by atomically replacing the file and reloading lmt-server.

A failed reload keeps the previous credential active.

## Agent credentials

Agent credentials are centrally issued and revoked.

On a clean installation, issuing the first credential for a valid Node name also establishes that Node as an offline/unbound control-plane record. The Agent does not self-register; its first authenticated poll supplies observed state and establishes the durable installation binding.

A Node may have more than one active credential so rotation can overlap safely.

Typical workflow:

~~~text
lmt node credential issue mirror01 --label rotation-2026-09 --token-file ./mirror01.new
copy/atomically install the new file as /etc/lmt/agent.token
systemctl reload lmt-agent
lmt node credential list mirror01
verify the new credential last_used_at advanced
lmt node credential revoke mirror01 <old-id>
~~~

Do not revoke the old credential until the Agent has proven use of the new one.

## Secret handling

New tokens are Server-generated with at least 256 bits of random material.

The raw token is shown exactly once.

The Server stores only a digest.

Raw tokens must never appear in logs, metrics, list/show responses, or SQLite rows.

Credential issue responses are non-cacheable.

CLI token-file creation should be atomic and mode 0600.

## Revocation semantics

Revocation stops future authenticated requests.

It does not remotely terminate a synchronization process already running on the Agent host.

For emergency host compromise, revoke credentials and isolate/stop the host through normal system administration.

## Agent binding

A credential authenticates the Node name.

M3 separately binds that Node to a durable Agent installation ID.

A second installation using the same valid credential is rejected before dispatch.

Replacing hardware/reinstalling an Agent is explicit:

1. stop or isolate the old installation;
2. learn the new Agent installation ID from the binding-conflict diagnostic;
3. replace the binding through the operator CLI;
4. use the high-risk acknowledgement only if potentially executing old work still exists.

Binding replacement is not automatic failover.

## Legacy M2 inline credentials

M3 accepts a narrow alpha compatibility bridge for M2 server config.

Legacy Agent tokens may be imported only if the Node has no credential history at all.

A revoked DB credential must never be resurrected just because a stale inline token remains in server.toml.

Remove inline raw Agent credentials after a successful M3 upgrade.
