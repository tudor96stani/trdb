---
created: 2026-02-14
---
Biggest unknown right now is hot to define the interface of the file manager in terms of file identification.

Meaning, if it gets a `page_id` (`file_id + page_number)`, it needs to do the mapping between the file ID and the file name. But that is a *Catalog* type mapping, should not be the responsibility of the file manager.

Meanwhile, if we define it as a `file_name` param, who does the mapping? the buffer and above should only work with `PageIDs`.

One option is to pass a resolves that checks the catalog, but the catalog lives in the `Sys` module, not storage (and thus it is at a higher level). Unless we make the sys module available for all layers

But to be honest it is too early on to be able to define such a module

## Way forward
As a conclusion after much thinking, the only reasonable way:
**keep the mapping inside the storage system**.
- in `/file`, create a `FileCatalog` struct that will cache every `file_id => file_name` mapping
- storage manager creates an `Arc<FileCatalog>`, passes a clone to the `FileManager` constructor
- use `RwLock` inside the catalog to wrap map
- only storage manager updates the catalog (not file manager)