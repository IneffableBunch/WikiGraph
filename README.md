# WikiLink Graph Database

A high-performance graph database implementation specifically designed for Wikipedia link networks, created from the pagelinks.sql.gz data dump provided by wikimedia. This project provides efficient storage and retrieval of Wikipedia page links using a custom binary format with compression.

## Features

- Custom binary format for storing graph data
- Compressed edge storage using zlib
- Delta encoding with variable-length integers
- Bidirectional mapping between page titles and node IDs
- Fast neighbor lookup using binary search
- Memory-mapped file access for optimal performance

## Installation

```bash
pip install varint zlib line_profiler
```

## Usage

### Converting Wikipedia Data

```python
from wikilink import convert_wiki_jsonl

# Convert JSONL dump to binary format
convert_wiki_jsonl('links.json', 'wikigraph.bin', 'map.bin')
```

### Reading Graph Data

```python
from wikilink import WikiLinkReader

# Initialize reader
reader = WikiLinkReader('wikigraph.bin', 'map.bin')

# Get neighbors for a node
neighbors = reader.get_neighbors(253)

# Get page title for a node ID
title = reader.get_title(253)

# Clean up
reader.close()
```

## File Format

### Binary Format Structure
- Magic number: "WLINKNET"
- Header: Version (uint32) + Node count (uint32)
- Edge blocks: Size (uint32) + Compressed edge data
- Index: (node_id, position) pairs
- Index position: uint64 at end of file

### Map File Structure
- Tab-separated values
- Format: `node_id\ttitle\n`

## Performance

The implementation uses several optimization techniques:
- Memory-mapped files for fast random access
- Delta encoding for compact edge representation
- zlib compression for reduced storage size
- Binary search for quick block lookup
- Bidirectional mapping for efficient ID/title conversion

## Requirements

- Python 3.6+
- varint
- zlib
- line_profiler

## License

MIT License

## Authors

Aharon Ahdoot
