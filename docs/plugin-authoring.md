# Plugin authoring

Plugins are immutable bundles containing a manifest and one binary per supported platform. A bundle declares name, semantic version, protocol range, capabilities, entrypoints, platform metadata, and a SHA-256 digest. Installation validates the digest and unpacks into a versioned directory; activation changes a pointer and never overwrites the previous version.

HashiCorp go-plugin provides process bootstrap, protocol negotiation, AutoMTLS, log routing, and cleanup. Protobuf/gRPC in `api/orchigram/plugin/v1alpha1` is the business protocol.

Every call includes request, run, node, attempt, deadline, and idempotency identity. A plugin must:

- emit monotonically increasing stream sequence numbers;
- stop accepting work after shutdown begins;
- implement cancel where advertised;
- avoid logging secret maps or protobuf requests containing them;
- tolerate duplicate requests with the same idempotency key;
- report whether its remote provider honors idempotency.

The conformance suite covers normal completion, cancellation, timeout, crash, incompatible protocol, malformed streams, and duplicate delivery.

