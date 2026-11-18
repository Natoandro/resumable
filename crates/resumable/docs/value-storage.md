# Stored values and metadata

`StoredValue` keeps payload bytes opaque and carries a codec identifier and schema version beside
them. The default `JsonCodec` uses codec `serde-json` and schema version `1`, as defined in the
[v0.1 architecture](architecture.md). Applications can implement `ValueCodec` for another stable
format; codec IDs and schema versions must continue to select a compatible decoder after restart.

Defaults and public bounds are defined together in `resumable::limits`. The core limits each
encoded value to 16 MiB. It limits execution metadata to 64 labels, 64 UTF-8 bytes per key,
256 UTF-8 bytes per value, and 8 KiB total key and value bytes. These bounds apply to constructors
and deserialization. Oversized or invalid data returns an explicit error.

Store references to large artifacts rather than their contents. A step result can contain an
object key, URI, content hash, or application identifier; the external artifact must remain
available for as long as a resumed execution may need it. Treat inputs, results, and metadata as
potentially sensitive persisted data. The core does not log serialized payloads; storage adapters
and applications own encryption, access control, retention, and redaction of diagnostics.

Decoding a completed step is read-only. Unknown codecs, incompatible schema versions, corrupt
payload bytes, and decode errors are returned to the caller while the completed record remains
intact. The runner must not treat a decode failure as permission to rerun the completed effect.
An execution result is assigned only by the running-to-terminal finish transition; cancellation
does not retain a result.
