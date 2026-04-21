**A log of various technical decisions made during development**

## 260417-0640 Reorganize storage crates into a single one

#### PROBLEM 
Initial choice for project structure was to separate each major component (storage engine, query engine, etc) into separate small crates and define an `*-api` crate (e.g. `storage-api`) that acted as the gateway into that component. This might be too complex and overengineered.

#### DECISION
Simplify things by doing a single crate/component **now**. Aggregate existing crates under `crates/storage` into a single one and remove unnecessary structs, enums, error types, etc. 

#### REASONING
Overengineered. Too much complexity with no real benefit right now. Can be split up later if needed. 
