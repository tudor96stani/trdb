---
created: 2026-04-22
---

# Overview
This document goes over the structure and layout of data pages, along with some details on how they are actually implemented. 

# Slotted page - definition
The implementation uses the concept of slotted pages. A slotted page is a format for storing the entries in a fixed size array of bytes, allowing for easy random access and internal organization without exposing the inner works of the structure.

The overall structure looks, visually, similar to this:
 ```text
   ↓ page_start
   ┌───────────────────────────────────────────────────────────────┐
   │ Page Header (contains slot_count, free space ptrs, etc.)      │
   ├───────────────────────────────────────────────────────────────┤
   │ Tuple Data Region (grows downwards)                           │
   │   records / row fragments                                     │
   │   variable sized                                              │
   │   aligned upwards                                             │
   ├───────────────────────────────────────────────────────────────┤
   │ Free Space                                                    │
   ├───────────────────────────────────────────────────────────────┤
   │ Slot Array Region (grows upwards)                             │
   │   fixed-size SLOT_SIZE entries                                │
   │   indexed logically left-to-right,                            │
   │   stored physically right-to-left                             │
   └───────────────────────────────────────────────────────────────┘

                                                           page_end ↑
 ```


In our case, the `PAGE_SIZE` is fixed to 4096 bytes - a pretty standard length.
It is comprised of 4 logical regions, placed in the following order:
- **Page header**
    - The page header is a 96 byte array placed at offset 0 in the page. It is used to store metadata about the page, such as slot count, the page type, page identifier, pointers to various locations in the page, etc. 
- **Data region**
    - This is the region that stores the actual row data. Each row is placed as a contiguous array of bytes. It is not mandatory for rows to be placed one after another (fragmentation can occur). The physical placement of the rows on the page is not relevant and is not exposed to the outside callers.
    - Grows inwards (left to right): first row is appended right after the header, subsequent rows being placed at ever increasing offsets. When page is fragmented, new rows might be placed in free chunks of space between existing ones.
- **Free space**
    - Represents the free area of the page. Shrinks as new rows appear. Pointers to `free_start` and `free_end` keep track of its size and placement.
- **Slot array**
    - The region storing the slot array of the data page. Each slot contains a pointer to the row it represents, along with the length of the row. Each valid row in the page has a corresponding slot. Rows are only requested from external callers via their **slot id** - the index of the slot in the array (0 indexed).
    - Grows inwards (right to left): first slot is placed at the very end of the page, subsequent slots are placed at ever decreasing offsets. Slot count value from header is used to determine the entire size of the array, as slot entries have a fixed size (4 bytes). Rightmost entry has index 0, rightmost - 1 has index 1 and so on (so indexing goes right to left as well).
    - Deletion of rows does not correspond to deletion of slots - as external entities refernce a row via its slot id (index), once a row gets assigned an ID it can never change. Instead, deletion causes slots to become invalidated, but does not shift the array to overwrite them.

## Flavors
There are (or will be) two main types of slotted pages: **heap** and **index**. Structurally, they are the same, 

# Implementation
