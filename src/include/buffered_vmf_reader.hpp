//===----------------------------------------------------------------------===//
//                         DuckDB
//
// buffered_vmf_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/enums/file_compression_type.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/mutex.hpp"
#include "vmf_common.hpp"
#include "vmf_enums.hpp"

namespace duckdb {

struct BufferedVMFReaderOptions {
public:
	//! The format of the VMF
	VMFFormat format = VMFFormat::AUTO_DETECT;
	//! Whether record types in the VMF
	VMFRecordType record_type = VMFRecordType::AUTO_DETECT;
	//! Whether file is compressed or not, and if so which compression type
	FileCompressionType compression = FileCompressionType::AUTO_DETECT;
	//! Multi-file reader options
	MultiFileReaderOptions file_options;

public:
	void Serialize(Serializer &serializer) const;
	static BufferedVMFReaderOptions Deserialize(Deserializer &deserializer);
};

struct VMFBufferHandle {
public:
	VMFBufferHandle(idx_t buffer_index, idx_t readers, AllocatedData &&buffer, idx_t buffer_size);

public:
	//! Buffer index (within same file)
	const idx_t buffer_index;

	//! Number of readers for this buffer
	atomic<idx_t> readers;
	//! The buffer
	AllocatedData buffer;
	//! The size of the data in the buffer (can be less than buffer.GetSize())
	const idx_t buffer_size;
};

struct VMFFileHandle {
public:
	VMFFileHandle(unique_ptr<FileHandle> file_handle, Allocator &allocator);

	bool IsOpen() const;
	void Close();

	void Reset();
	bool RequestedReadsComplete();
	bool LastReadRequested() const;

	idx_t FileSize() const;
	idx_t Remaining() const;

	bool CanSeek() const;
	bool IsPipe() const;

	FileHandle &GetHandle();

	//! The next two functions return whether the read was successful
	bool GetPositionAndSize(idx_t &position, idx_t &size, idx_t requested_size);
	bool Read(char *pointer, idx_t &read_size, idx_t requested_size, bool &file_done, bool sample_run);
	//! Read at position optionally allows passing a custom handle to read from, otherwise the default one is used
	void ReadAtPosition(char *pointer, idx_t size, idx_t position, bool &file_done, bool sample_run,
	                    optional_ptr<FileHandle> override_handle = nullptr);

private:
	idx_t ReadInternal(char *pointer, const idx_t requested_size);
	idx_t ReadFromCache(char *&pointer, idx_t &size, idx_t &position);

private:
	//! The VMF file handle
	unique_ptr<FileHandle> file_handle;
	Allocator &allocator;

	//! File properties
	const bool can_seek;
	const idx_t file_size;

	//! Read properties
	idx_t read_position;
	atomic<idx_t> requested_reads;
	atomic<idx_t> actual_reads;
	atomic<bool> last_read_requested;

	//! Cached buffers for resetting when reading stream
	vector<AllocatedData> cached_buffers;
	idx_t cached_size;
};

class BufferedVMFReader {
public:
	BufferedVMFReader(ClientContext &context, BufferedVMFReaderOptions options, string file_name);

	void OpenVMFFile();
	void Reset();

	bool HasFileHandle() const;
	bool IsOpen() const;

	BufferedVMFReaderOptions &GetOptions();

	VMFFormat GetFormat() const;
	void SetFormat(VMFFormat format);

	VMFRecordType GetRecordType() const;
	void SetRecordType(VMFRecordType type);

	const string &GetFileName() const;
	VMFFileHandle &GetFileHandle() const;

public:
	//! Insert/get/remove buffer (grabs the lock)
	void InsertBuffer(idx_t buffer_idx, unique_ptr<VMFBufferHandle> &&buffer);
	optional_ptr<VMFBufferHandle> GetBuffer(idx_t buffer_idx);
	AllocatedData RemoveBuffer(VMFBufferHandle &handle);

	//! Get a new buffer index (must hold the lock)
	idx_t GetBufferIndex();
	//! Set line count for a buffer that is done (grabs the lock)
	void SetBufferLineOrObjectCount(VMFBufferHandle &handle, idx_t count);
	//! Throws a parse error that mentions the file name and line number
	void ThrowParseError(idx_t buf_index, idx_t line_or_object_in_buf, yyvmf_read_err &err, const string &extra = "");
	//! Throws a transform error that mentions the file name and line number
	void ThrowTransformError(idx_t buf_index, idx_t line_or_object_in_buf, const string &error_message);

	//! Scan progress
	double GetProgress() const;

private:
	idx_t GetLineNumber(idx_t buf_index, idx_t line_or_object_in_buf);

private:
	ClientContext &context;
	BufferedVMFReaderOptions options;

	//! File name
	const string file_name;
	//! File handle
	unique_ptr<VMFFileHandle> file_handle;

	//! Next buffer index within the file
	idx_t buffer_index;
	//! Mapping from batch index to currently held buffers
	unordered_map<idx_t, unique_ptr<VMFBufferHandle>> buffer_map;

	//! Line count per buffer
	vector<int64_t> buffer_line_or_object_counts;
	//! Whether any of the reading threads has thrown an error
	bool thrown;

public:
	mutable mutex lock;
	MultiFileReaderData reader_data;
};

} // namespace duckdb
