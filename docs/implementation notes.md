---
created: 2026-04-21
---
**This mostly contains random notes taken during development. Ignore Origin references for each note, mostly used for linking in my main Obsidian vault**

## 260218-1840 changes to server client implementation
Initial test implementation had the client close the connection after the first query - changed it so that the client loops as well. Also made quite a lot of changes to the server, the most important being:
- server now runs in a loop in the `client_handle` method, to be able to read data multiple times from the same socket
- separate the read and write channels for the client connection - not fully needed right now, but will help once we are able to stream the result back to the client.
#### Origin
- [[26-4]] 260218-1840 p25
---
## 260218-1727 tokio select!
The `select!` macro can be used to run multiple concurrent branches, returning when the first completes and cancelling the remaining ones.
**Use case:** in the main function, running the server functions that await for connections and start the processing of incoming queries + a branch to wait for a `SIGTERM`. If the program receives a shutdown request, it will enter this branch, complete the future and stop processing new requests. Part of the graceful shutdown flow.
#### Origin
- [[26-4]] 260218-1727 p25
---
## 260218-1308 testing, cfg and buffer
Testing race conditions is tricky - I don't think I need to cover all of them in the buffer tests, but at least a few of them.
### Options considered
- rewriting the buffer in the tests module (by hiding it behind a trait) with extra hooks and gates
	- + very granular control
	- + no pollution of the main implementation
	- - code duplication
- using a lib like `loom`
	- + can cover all (or almost all) scenarios automatically
	- - kinda tricky to setup, a bit overkill
### Middleground
- `use` statement decorated with `#[cfg(test)]`
- add a field in buffer to hold a `Barrier`, hide it behind `#[cfg(test)]` (field declaration + ctor + setter)
- add a pause method that is only under `#[cfg(test)]`
	- an `if wait` on the barrier, hidden behind `#[cfg(test)]`
	- under non-test env, empty body
	- method decorated with `inline always`
	- method called at stopping points
	- since this is decorated with both cfg test and inline, the compiler should remove it for non-test builds, since the method body itself will be empty for those scenarios
example of usage:
```rust
#[cfg(test)]  
impl<F: FileManager> BufferManager<F> {    
    #[inline(always)]  
    fn test_pause(&self) {  
        #[cfg(test)]  
        if let Some(b) = &self.hooks.get() {  
            b.wait();  
        }  
        // non-test: nothing  
    }   
}
```
#### Origin
- [[26-4]] 260218-1308 p24
---
## 260214-1854 client-server
As I said in **260214-1852 small reorg of crates**, split the client and the server. Made them communicate via a TCP socket, the client sending the request to the server and getting back a response. Some notes here:
- currently client does not send query as there is no query processor
- server only sends back a single row
- server waits for the worker thread to finish before it starts streaming back the results
	- ideally here, the server should start streaming results as soon as it gets them, even if more are coming from the worker
	- need to define a `MAX_PACKET_SIZE` for this and fill a packet, ship it and repeat until the worker signals `DONE`
- ideally the two binaries will only have one shared reference, the one defining the TCP protocol
	- client should depend on nothing else (from the engine)
- long way to go, but so far looking good

### Origin
- [[26-4]] 260214-1854 p11

---
## 260214-1852 small reorg of crates
I introduced the `trdbcmd` binary, basically the CLI client - I created the binary crate under `src/apps`. I also moved the main server binary there - it looks ok this way, `/crates` will only contains libraries used by the engine

### Origin
- [[26-4]] 260214-1852 p11

---
## 260213-2043 async
Started implementing all of this async + threads part in TRDB, seems to be working? Also created a `trdbcmd` binary for the command line client, and did what was described earlier for the server - it starts a loop in which it awaits connections, handles the async connection asynchronously, waits on a semaphore for an available worker thread, then once available it moves the work there - the worker threads inserts a new row, reads it and returns it, the client handler sends it back over the TCP socket and closes the connection.
Some more debugging via logs is needed to ensure work is actually scheduled as expected, but I expect it works fine.

### Origin
- [[26-4]] 260213-2043 p7

---
## 260213-0633 async or threads
With threads, the OS can switch between them at any given time; with async, the switch only happens when one tasks yields control back to the executor

Where does this leave me for TRDB? TBH not 100% sure, the flow should be:
```
server starts -> configures state -> waits for connection
							                 |
								             \/
joins handle <-  triggers thread or  <-  accepts connection 
when done		task for connection   
```

Normally, this sounds like threads - a longer running set of operations that run in parallel. But would be nice if during I/O to give back the thread so it is not busy waiting

The best might be some combo? Use tokio to define a pool of threads that wait for connections and interact with the TCP stream. Once a connection is made, read the input async and then request the query to be executed on a thread from the blocking thread pool - each threat will handle 1 client request. The tokio thread can go back in the pool. The worker threads are blocking - they will wait on I/O while processing the query. There will be X worker threads and we will have a semaphore to limit the number of concurrent connections.

In theory we could do a SQL Server specific thing, where we have this cooperative approach where a thread will be returned back to the pool once a blocking I/O operation is encountered, but this is far more complex. This might be cool, but a bit overkill for now tbh. So we are fine with worker threads blocking during I/O, since each query will get its own thread.

**(Later)**
Did a small test project with this: the server does a `loop {listener.await}` then does `tokio::spawn` to call a `process_connection` async. In there, it reads the data from the stream waits on a semaphore for the number of concurrent workers and once it gains access calls `tokio::spawn_blocking` to run the actual worker operation.
This leaves the server able to receive as many connections in parallel as possible, but only process a given number of them.

### Origin
- [[26-4]] 260213-0633 p5

---
## 260205-1813 current flow for buffer manager, reasoning
The current flow is to have the buffer own the frames that store the pages, then handing out references to them. The refs, protected by a read or write guard, allow callers to interact with the page directly. 
Furthermore, the buffer will provide a frame when a new page is needed (rather than the caller creating the page) => this is actually quite nice.
This means that the caller is responsible for clearing out any possible remaining data on the page from the frame, as well as setting the appropriate fields. While this might seem annoying, I don't think the buffer should be responsible for dealing with the contents of the page.

Still unsure whether we need to notify the buffer that we are done with the updates to a page. We could just set it as dirty when obtaining a write reference and just assume that the caller has done this on purpose. Maybe for now, until we have the WAL and auto-flushing of dirty pages, we can notify the buffer to write it to disk and 
clear the dirty flag. 

### Origin
- [[26-3]] 260205-1813 p21

---
## 260203-1804 notes on storage
Some things about the storage module
- need to define ownership model for the 3 services
- most likely best approach is to have some *context* instance that holds there 3 actual instances
- then of of them can only use references/views into them
- storage manager should have, realistically, no internal state (except references to the other 2)
- for starters, context can be minimal: only creates those instances and owns them
- also decide who creates and owns the `FileCatalog`? in theory i think it should be the context
- so the context will look like this:
```rust
file_manager: Arc<F> where F: FileManager
buffer: Arc<BufferManager<F>>
storage: Arc<StorageManager>
file_catalog: Arc<FileCatalog>
```
- maybe use `Environment` instead of `Context`, since context can mean for a request, while this is long lived
- look into where do we store this env? so that it really is long lived?
	- for now, just place it in the binary and we will see

### Origin 
- [[26-3]] 260203-1804 p14