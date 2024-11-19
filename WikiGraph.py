import struct
import mmap
import varint  # for variable integer encoding
import zlib
import json
from line_profiler import profile

class BiDict:
    def __init__(self):
        self.forward = {}
        self.backward = {}

    def __setitem__(self, key, value):
        self.forward[key] = value
        self.backward[value] = key

    def __getitem__(self, key):
        try:
            return self.forward[key]
        except KeyError:
            return self.backward[key]

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __contains__(self, key):
        return key in self.forward or key in self.backward

    def __len__(self):
        return len(self.forward)

    def __str__(self):
        return str(self.forward)
    def items(self):
        """Return forward dictionary items"""
        return self.forward.items()

class WikiLinkWriter:
    MAGIC = b"WLINKNET"
    VERSION = 1
    BLOCK_SIZE = 16384
    LIMIT = None

    def __init__(self, output_path, map_path):
        self.outfile = open(output_path, 'wb')
        self.mapfile = open(map_path, 'w', encoding='utf-8')
        self.node_map = {}  # title -> node_id
        self.current_node_id = 0
        self.index = []

    def write_header(self):
        # Placeholder for header, will be updated later
        self.outfile.write(self.MAGIC)
        self.outfile.write(struct.pack('<II', self.VERSION, 0))  # Node count will be updated later

    def process_jsonl_dump(self, jsonl_file):
        """Process Wikipedia JSONL file"""
        self.write_header()
        current_pos = self.outfile.tell()

        # First pass: collect all node IDs and their edges
        nodes_with_edges = {}  # node_id -> edge_list
        for i, line in enumerate(jsonl_file):
            # if i >= 1000:
            #     break
            data = json.loads(line)
            for node_title, linked_titles in data.items():
                if node_title not in self.node_map:
                    self.node_map[node_title] = self.current_node_id
                    self.mapfile.write(f"{self.current_node_id}\t{node_title}\n")
                    self.current_node_id += 1
                
                node_id = self.node_map[node_title]
                edge_ids = []
                for title in linked_titles:
                    if title not in self.node_map:
                        self.node_map[title] = self.current_node_id
                        self.mapfile.write(f"{self.current_node_id}\t{title}\n")
                        self.current_node_id += 1
                    edge_ids.append(self.node_map[title])
                
                if edge_ids:  # Only store nodes that actually have edges
                    nodes_with_edges[node_id] = edge_ids

        # Write only nodes that have edges
        for node_id, edge_ids in nodes_with_edges.items():
            # print(f"Writing edges for node {node_id}: {edge_ids}")
            encoded_edges = self.encode_edges(edge_ids)
            compressed = zlib.compress(encoded_edges)
            self.outfile.write(struct.pack('<I', len(compressed)))
            self.outfile.write(compressed)
            self.index.append((node_id, current_pos))
            current_pos = self.outfile.tell()

        # Write index
        index_pos = current_pos
        # print(f"Writing index at position: {index_pos}")
        for node_id, pos in sorted(self.index):
            # print(f"Writing index entry: node_id={node_id}, pos={pos}")
            self.outfile.write(struct.pack('<QQ', node_id, pos))

        # Write index position at end of file
        self.outfile.write(struct.pack('<Q', index_pos))
        # print(f"Wrote index position {index_pos} at file end")

        # Update node count in header
        self.outfile.seek(8)
        self.outfile.write(struct.pack('<I', self.current_node_id))
        # print(f"Updated header with node count: {self.current_node_id}")

    def encode_edges(self, edges):
        """Encode edge list with delta + varint compression"""
        encoded = bytearray()
        
        # Write number of edges
        encoded.extend(varint.encode(len(edges)))
        
        # Sort and encode deltas
        sorted_edges = sorted(edges)
        prev = 0
        for edge in sorted_edges:
            delta = edge - prev
            encoded.extend(varint.encode(delta))
            prev = edge
            
        return bytes(encoded)


    def close(self):
        self.outfile.close()
        self.mapfile.close()
    
    # Add this verification method to the writer
    def verify_encoding(self, original_edges, encoded_data):
        """Verify that edges can be correctly decoded"""
        pos = 0
        decoded_edges = []
        
        # Read number of edges
        num_edges = varint.decode_bytes(encoded_data[pos:])
        pos += len(varint.encode(num_edges))
        
        # Decode edges
        prev = 0
        for _ in range(num_edges):
            delta = varint.decode_bytes(encoded_data[pos:])
            pos += len(varint.encode(delta))
            prev += delta
            decoded_edges.append(prev)
        
        # Compare original and decoded edges
        original_sorted = sorted(original_edges)
        if original_sorted != decoded_edges:
            # print("Encoding verification failed!")
            # print(f"Original (sorted): {original_sorted}")
            # print(f"Decoded: {decoded_edges}")
            return False
        return True

class WikiLinkReader:
    def __init__(self, filename, map_path):
        self.file = open(filename, 'rb')
        self.mmap = mmap.mmap(self.file.fileno(), 0, access=mmap.ACCESS_READ)

        # Verify magic number
        if self.mmap[:8] != WikiLinkWriter.MAGIC:
            raise ValueError("Invalid file format")

        # Read header
        self.version, self.node_count = struct.unpack('<II', self.mmap[8:16])

        # Read index position
        self.mmap.seek(-8, 2)
        self.index_pos = struct.unpack('<Q', self.mmap.read(8))[0]

        # Load index
        self.load_index()

        # Load title map
        self.title_map = self.load_title_map(map_path)

    def load_index(self):
        self.index = []
        pos = self.index_pos
        # print(f"Loading index from position: {self.index_pos}")  # Debug print
        # print(f"Total file size: {len(self.mmap)}")  # Debug print
        
        while pos < len(self.mmap) - 8:  # -8 for final index position
            try:
                node_id, block_pos = struct.unpack('<QQ', self.mmap[pos:pos+16])
                # print(f"Loaded index entry: node_id={node_id}, block_pos={block_pos}")  # Debug print
                self.index.append((node_id, block_pos))
                pos += 16
            except Exception as e:
                # print(f"Error reading index at position {pos}: {e}")
                break
        
        # print(f"Loaded {len(self.index)} index entries")  # Debug print


    def load_title_map(self, map_path):
        bidict = BiDict()
        with open(map_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    node_id, title = line.strip().split('\t')
                    bidict[int(node_id)] = title
                except:
                    print(line)
        return bidict

    def find_block(self, node_id):
        """Binary search for the correct block"""
        if not self.index:
            raise ValueError("Index is empty - no blocks found")
                
        # print(f"Searching for node_id {node_id} in index")
        # print(f"Index contains {len(self.index)} entries")
        
        # Binary search for exact match only
        left = 0
        right = len(self.index) - 1
        
        while left <= right:
            mid = (left + right) // 2
            current_id = self.index[mid][0]
            
            if current_id == node_id:
                return self.index[mid][1]
            elif current_id < node_id:
                left = mid + 1
            else:
                right = mid - 1
                
        return None  # No exact match found

    @profile
    def get_neighbors(self, node_id):
        """Get all neighbors for a node"""
        block_pos = self.find_block(node_id)
        
        if block_pos is None:
            # print(f"No edges found for node {node_id}")
            return []
            
        # print(f"Looking for node {node_id}, found block at position {block_pos}")
        
        # Read block size
        block_size = struct.unpack('<I', self.mmap[block_pos:block_pos+4])[0]
        # print(f"Block size: {block_size}")

        # Read and decompress block
        compressed = self.mmap[block_pos+4:block_pos+4+block_size]
        data = zlib.decompress(compressed)
        
        # First byte should be the number of edges
        num_edges = varint.decode_bytes(data)
        # print(f"Number of edges: {num_edges}")
        
        # If this is an empty block, return empty list
        if num_edges == 0:
            return []
            
        # If we have edges, decode them
        edges = []
        pos = len(varint.encode(num_edges))  # Skip past the edge count
        prev = 0
        
        for _ in range(num_edges):
            delta = varint.decode_bytes(data[pos:])
            pos += len(varint.encode(delta))
            prev += delta
            edges.append(prev)
        
        return edges

    def get_title(self, node_id):
        """Get the title for a given node ID"""
        return self.title_map.get(node_id, None)

    def close(self):
        self.file.close()
        self.mmap.close()

# Usage example:
def convert_wiki_jsonl(jsonl_path, output_path, map_path):
    writer = WikiLinkWriter(output_path, map_path)
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        writer.process_jsonl_dump(f)
    writer.close()

if __name__ == '__main__':
    # Convert dump to binary format
    convert_wiki_jsonl('links_temp_16000.json', 'wikigraph.bin', 'map.bin')

    # Read and query
    reader = WikiLinkReader('wiki.graph', 'map.bin')

    reader.get_neighbors(253)
