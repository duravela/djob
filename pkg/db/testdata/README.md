# SDK extraction fixtures

`pre-sdk-contracts.gob` and `pre-sdk-contracts.json` were generated before moving the job value types from `pkg/db` into the SDK. They contain the same commands, run, event and resource in both encodings, with populated configuration, action, activation, ownership and cancellation metadata.

The compatibility test decodes the old Gob data using the engine aliases and compares its JSON with the independently captured old field representation. Keep these fixtures unchanged when modifying the SDK; a format change needs an explicit compatibility decision.
