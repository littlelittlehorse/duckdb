//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/single_file_block_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class DatabaseInstance;

//! SingleFileBlockManager is an implementation for a BlockManager which manages blocks in a single file
class SingleFileBlockManager : public BlockManager {
	friend class FileBuffer;
	    //! The location in the file where the block writing starts
	static constexpr uint64_t BLOCK_START = Storage::FILE_HEADER_SIZE * 3;

    // ! The new starting position of the block after the meta information is removed
	static constexpr uint64_t NEW_BLOCK_START = 0;

public:
	SingleFileBlockManager(DatabaseInstance &db, string path, bool read_only, bool create_new, bool use_direct_io);

	SingleFileBlockManager(DatabaseInstance &db, string path, string nvm_path, bool read_only, bool create_new, bool use_direct_io);

	void StartCheckpoint() override;
	//! Creates a new Block using the specified block_id and returns a pointer
	unique_ptr<Block> CreateBlock(block_id_t block_id) override;
	//! Return the next free block id
	block_id_t GetFreeBlockId() override;
	//! Return the next nvm free block id
	block_id_t GetNvmFreeBlockId();
	//! Return the next ssd free block id
	block_id_t GetSsdFreeBlockId();
	block_id_t GetNvmMinHotCountsId(block_id_t id);
	//! Returns whether or not a specified block is the root block
	bool IsRootBlock(block_id_t root) override;
	//! Mark a block as modified
	void MarkBlockAsModified(block_id_t block_id) override;
	//! Increase the reference count of a block. The block should hold at least one reference
	void IncreaseBlockReferenceCount(block_id_t block_id) override;
	//! Return the meta block id
	block_id_t GetMetaBlock() override;
	//! Read the content of the block from disk
	void Read(Block &block) override;
	void DirectNvmRead(Block &block) ;
	void DirectSsdRead(Block &block) ;
	//! Read the content of the block from nvm disk
	void NvmRead(Block &block);
	//! Write the given block to disk
	void Write(FileBuffer &block, block_id_t block_id) override;
	void DirectNvmWrite(FileBuffer &block, block_id_t block_id);
	void DirectSsdWrite(FileBuffer &block, block_id_t block_id);
	//! Write the given block to nvm disk
	void NvmWrite(FileBuffer &block, block_id_t block_id);
	//! Write the header to disk, this is the final step of the checkpointing process
	void WriteHeader(DatabaseHeader header) override;
	void NvmWriteHeader(DatabaseHeader header) override;

	//! Returns the number of total blocks
	idx_t TotalBlocks() override {
		return max_block;
	}
	//! Returns the number of free blocks
	idx_t FreeBlocks() override {
		return free_list.size();
	}
	//! Load the free list from the file
	void LoadFreeList();

private:
	void Initialize(DatabaseHeader &header);

	//! Return the blocks to which we will write the free list and modified blocks
	vector<block_id_t> GetFreeListBlocks();

private:
	DatabaseInstance &db;
	//! The active DatabaseHeader, either 0 (h1) or 1 (h2)
	uint8_t active_header;
	//! The path where the file is stored
	string path;
    //! The path where the file is stored
    string nvm_path;
	//! The file handle
	unique_ptr<FileHandle> handle;
    // ! The nvm file hand
    unique_ptr<FileHandle> nvm_handle;
	//! The buffer used to read/write to the headers
	FileBuffer header_buffer;
	//! The list of free blocks that can be written to currently
	set<block_id_t> free_list;
	set<block_id_t> nvm_free_list;
	mutex nvm_free_list_lock;
	set<block_id_t> ssd_free_list;
	mutex ssd_free_list_lock;
	//! The list of multi-use blocks (i.e. blocks that have >1 reference in the file)
	//! When a multi-use block is marked as modified, the reference count is decreased by 1 instead of directly
	//! Appending the block to the modified_blocks list
	unordered_map<block_id_t, uint32_t> multi_use_blocks;
	//! The location of the block persistent storage, either 0 (SSD) or 1 (NVM), not in the unordered_map is uncertain.
	unordered_map<block_id_t, uint8_t> block_location;
	mutex block_location_lock;
	//! The heat value of each block. The greater the heat value, the higher the heat value.
	unordered_map<block_id_t, uint64_t> block_hot_counts;
	mutex block_hot_counts_lock;
	//! Global block_id specifies the block_id of the mapping to the ssd.
	unordered_map<block_id_t, uint64_t> ssd_block;
	mutex ssd_block_lock;
	//! Global block_id specifies the block_id of the mapping to the nvm.
	unordered_map<block_id_t, uint64_t> nvm_block;
	mutex nvm_block_lock;
	//! The list of blocks that will be added to the free list
	unordered_set<block_id_t> modified_blocks;
	//! The current meta block id
	block_id_t meta_block;
	//! The current maximum block id, this id will be given away first after the free_list runs out
	block_id_t max_block;
	block_id_t nvm_max_block;
	block_id_t ssd_max_block;
	//! The block id where the free list can be found
	block_id_t free_list_id;
	//! The current header iteration count
	uint64_t iteration_count;
	//! Whether or not the db is opened in read-only mode
	bool read_only;
	//! Whether or not to use Direct IO to read the blocks
	bool use_direct_io;
	mutex find_nvm_block_lock;
	const block_id_t MaxNvmBlockCount = 100;
};
} // namespace duckdb
