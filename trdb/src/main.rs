//! # TRDB Server
//!
//! This is the main entry point for the **TRDB** database server.
//!
//! The server is composed of multiple internal components organized under
//! the `/crates` directory of this workspace:
//!
//! - `/storage`: Core storage engine handling data persistence and retrieval.
//!
//! This binary is the main executable for the database engine.

#![allow(unused)] // Silence compiler warnings about unused code until they are referenced in main binary. TODO: remove this

use crate::engine_environment::EngineEnvironment;
use page::page_id;
use page::page_id::PageId;
use page::page_type::PageType;

mod engine_environment;

fn main() {
    let e = EngineEnvironment::new();

    let page_id = PageId::new(1, 1);
    let mut page = e.storage.new_page(page_id);
    let Ok(()) = page.initialize(page_id, PageType::Unsorted) else {
        panic!("cannot set page type")
    };

    let Ok(insert_plan) = page.plan_insert(100) else {
        panic!("insert plan failed");
    };

    match page.insert_heap(insert_plan, vec![1u8; 100]) {
        Ok(_) => {}
        Err(e) => panic!("insert failed: {}", e),
    }

    let Ok(row) = page.row(0) else {
        panic!("row 0 failed");
    };

    println!("{:?}", row);
}
