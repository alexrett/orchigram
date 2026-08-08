# Flow data contracts

`Flow.spec.inputSchema` and every plugin action descriptor use JSON Schema
Draft 2020-12. The complete schema is compiled for runtime validation. External
`$ref` URLs are rejected so plugin installation and Flow compilation never
perform implicit network access; local `#` references remain valid.

```yaml
spec:
  inputSchema:
    type: object
    properties:
      issue:
        type: object
        properties:
          number: {type: integer}
        required: [number]
        additionalProperties: false
    required: [issue]
    additionalProperties: false
```

Before a Flow is stored, the compiler resolves the exact active action
descriptor and checks:

- literal `with` fields after removing Orchigram-owned `mappings` and
  `secretRefs`;
- required fields after literal values and mapping targets are combined;
- `input.*` sources against `spec.inputSchema`;
- `nodes.<id>.*` sources against predecessor output schemas;
- JSON Pointer targets against the destination config schema;
- source and destination type compatibility;
- edge conditions against the source node output schema when CEL's ordinary
  checker would otherwise return `dyn`.

Static path/type inspection deliberately understands `type`, `properties`,
`required`, `additionalProperties`, array `items`, and local references. The
runtime validator still evaluates the complete Draft 2020-12 document,
including composition and conditional keywords. A path through an explicitly
open schema region is accepted with a `warning` diagnostic and is validated
again using the pinned schema at execution. Closed-schema unknown fields,
missing paths, incompatible types, and non-boolean conditions are errors and
prevent storage.

Every accepted plan contains its Flow input schema and each non-core node's
canonical config/input/output schemas plus the immutable plugin contract
digest. Runtime config is validated after template rendering, mappings, host
field injection, and secret-reference removal. Input is validated before the
trigger receipt is acknowledged. Output is validated before downstream nodes
can consume it. Diagnostics contain paths and stable codes, never values.
