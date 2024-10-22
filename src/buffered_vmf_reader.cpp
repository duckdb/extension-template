#include "buffered_vmf_reader.hpp"

#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"

#include <utility>

namespace duckdb {

VMFBufferHandle::VMFBufferHandle(idx_t buffer_index_p, idx_t readers_p, AllocatedData &&buffer_p, idx_t buffer_size_p)
    : buffer_index(buffer_index_p), readers(readers_p), buffer(std::move(buffer_p)), buffer_size(buffer_size_p) {
}

VMFFileHandle::VMFFileHandle(unique_ptr<FileHandle> file_handle_p, Allocator &allocator_p)
    : file_handle(std::move(file_handle_p)), allocator(allocator_p), can_seek(file_handle->CanSeek()),
      file_size(file_handle->GetFileSize()), read_position(0), requested_reads(0), actual_reads(0),
      last_read_requested(false), cached_size(0) {
}

bool VMFFileHandle::IsOpen() const {
	return file_handle != nullptr;
}

void VMFFileHandle::Close() {
	if (IsOpen() && !file_handle->IsPipe()) {
		file_handle->Close();
		file_handle = nullptr;
	}
}

void VMFFileHandle::Reset() {
	D_ASSERT(RequestedReadsComplete());
	read_position = 0;
	requested_reads = 0;
	actual_reads = 0;
	last_read_requested = false;
	if (IsOpen() && CanSeek()) {
		file_handle->Reset();
	}
}

bool VMFFileHandle::RequestedReadsComplete() {
	return requested_reads == actual_reads;
}

bool VMFFileHandle::LastReadRequested() const {
	return last_read_requested;
}

idx_t VMFFileHandle::FileSize() const {
	return file_size;
}

idx_t VMFFileHandle::Remaining() const {
	return file_size - read_position;
}

bool VMFFileHandle::CanSeek() const {
	return can_seek;
}

bool VMFFileHandle::IsPipe() const {
	return file_handle->IsPipe();
}

FileHandle &VMFFileHandle::GetHandle() {
	return *file_handle;
}

bool VMFFileHandle::GetPositionAndSize(idx_t &position, idx_t &size, idx_t requested_size) {
	D_ASSERT(requested_size != 0);
	if (last_read_requested) {
		return false;
	}

	position = read_position;
	size = MinValue<idx_t>(requested_size, Remaining());
	read_position += size;

	requested_reads++;
	if (size == 0) {
		last_read_requested = true;
	}

	return true;
}

void VMFFileHandle::ReadAtPosition(char *pointer, idx_t size, idx_t position, bool &file_done, bool sample_run,
                                    optional_ptr<FileHandle> override_handle) {
	if (size != 0) {
		auto &handle = override_handle ? *override_handle.get() : *file_handle.get();
		if (can_seek) {
			handle.Read(pointer, size, position);
		} else if (sample_run) { // Cache the buffer
			handle.Read(pointer, size, position);

			cached_buffers.emplace_back(allocator.Allocate(size));
			memcpy(cached_buffers.back().get(), pointer, size);
			cached_size += size;
		} else {
			if (!cached_buffers.empty() || position < cached_size) {
				ReadFromCache(pointer, size, position);
			}

			if (size != 0) {
				handle.Read(pointer, size, position);
			}
		}
	}

	const auto incremented_actual_reads = ++actual_reads;
	if (incremented_actual_reads > requested_reads) {
		throw InternalException("VMFFileHandle performed more actual reads than requested reads");
	}

	if (last_read_requested && incremented_actual_reads == requested_reads) {
		file_done = true;
	}
}

bool VMFFileHandle::Read(char *pointer, idx_t &read_size, idx_t requested_size, bool &file_done, bool sample_run) {
	D_ASSERT(requested_size != 0);
	if (last_read_requested) {
		return false;
	}

	if (can_seek) {
		read_size = ReadInternal(pointer, requested_size);
		read_position += read_size;
	} else if (sample_run) { // Cache the buffer
		read_size = ReadInternal(pointer, requested_size);
		if (read_size > 0) {
			cached_buffers.emplace_back(allocator.Allocate(read_size));
			memcpy(cached_buffers.back().get(), pointer, read_size);
		}
		cached_size += read_size;
		read_position += read_size;
	} else {
		read_size = 0;
		if (!cached_buffers.empty() || read_position < cached_size) {
			read_size += ReadFromCache(pointer, requested_size, read_position);
		}
		if (requested_size != 0) {
			read_size += ReadInternal(pointer, requested_size);
		}
	}

	if (read_size == 0) {
		last_read_requested = true;
		file_done = true;
	}

	return true;
}

idx_t VMFFileHandle::ReadInternal(char *pointer, const idx_t requested_size) {
	// Deal with reading from pipes
	idx_t total_read_size = 0;
	while (total_read_size < requested_size) {
		auto read_size = file_handle->Read(pointer + total_read_size, requested_size - total_read_size);
		if (read_size == 0) {
			break;
		}
		total_read_size += read_size;
	}
	return total_read_size;
}

idx_t VMFFileHandle::ReadFromCache(char *&pointer, idx_t &size, idx_t &position) {
	idx_t read_size = 0;
	idx_t total_offset = 0;

	idx_t cached_buffer_idx;
	for (cached_buffer_idx = 0; cached_buffer_idx < cached_buffers.size(); cached_buffer_idx++) {
		auto &cached_buffer = cached_buffers[cached_buffer_idx];
		if (size == 0) {
			break;
		}
		if (position < total_offset + cached_buffer.GetSize()) {
			idx_t within_buffer_offset = position - total_offset;
			idx_t copy_size = MinValue<idx_t>(size, cached_buffer.GetSize() - within_buffer_offset);
			memcpy(pointer, cached_buffer.get() + within_buffer_offset, copy_size);

			read_size += copy_size;
			pointer += copy_size;
			size -= copy_size;
			position += copy_size;
		}
		total_offset += cached_buffer.GetSize();
	}

	return read_size;
}

BufferedVMFReader::BufferedVMFReader(ClientContext &context, BufferedVMFReaderOptions options_p, string file_name_p)
    : context(context), options(std::move(options_p)), file_name(std::move(file_name_p)), buffer_index(0),
      thrown(false) {
}

void BufferedVMFReader::OpenVMFFile() {
	lock_guard<mutex> guard(lock);
	if (!IsOpen()) {
		auto &fs = FileSystem::GetFileSystem(context);
		auto regular_file_handle = fs.OpenFile(file_name, FileFlags::FILE_FLAGS_READ | options.compression);
		file_handle = make_uniq<VMFFileHandle>(std::move(regular_file_handle), BufferAllocator::Get(context));
	}
	Reset();
}

void BufferedVMFReader::Reset() {
	buffer_index = 0;
	buffer_map.clear();
	buffer_line_or_object_counts.clear();
	if (HasFileHandle()) {
		file_handle->Reset();
	}
}

bool BufferedVMFReader::HasFileHandle() const {
	return file_handle != nullptr;
}

bool BufferedVMFReader::IsOpen() const {
	if (HasFileHandle()) {
		return file_handle->IsOpen();
	}
	return false;
}

BufferedVMFReaderOptions &BufferedVMFReader::GetOptions() {
	return options;
}

VMFFormat BufferedVMFReader::GetFormat() const {
	return options.format;
}

void BufferedVMFReader::SetFormat(VMFFormat format) {
	D_ASSERT(options.format == VMFFormat::AUTO_DETECT);
	options.format = format;
}

VMFRecordType BufferedVMFReader::GetRecordType() const {
	return options.record_type;
}

void BufferedVMFReader::SetRecordType(duckdb::VMFRecordType type) {
	D_ASSERT(options.record_type == VMFRecordType::AUTO_DETECT);
	options.record_type = type;
}

const string &BufferedVMFReader::GetFileName() const {
	return file_name;
}

VMFFileHandle &BufferedVMFReader::GetFileHandle() const {
	D_ASSERT(HasFileHandle());
	return *file_handle;
}

void BufferedVMFReader::InsertBuffer(idx_t buffer_idx, unique_ptr<VMFBufferHandle> &&buffer) {
	lock_guard<mutex> guard(lock);
	buffer_map.insert(make_pair(buffer_idx, std::move(buffer)));
}

optional_ptr<VMFBufferHandle> BufferedVMFReader::GetBuffer(idx_t buffer_idx) {
	lock_guard<mutex> guard(lock);
	auto it = buffer_map.find(buffer_idx);
	return it == buffer_map.end() ? nullptr : it->second.get();
}

AllocatedData BufferedVMFReader::RemoveBuffer(VMFBufferHandle &handle) {
	lock_guard<mutex> guard(lock);
	auto it = buffer_map.find(handle.buffer_index);
	D_ASSERT(it != buffer_map.end());
	D_ASSERT(RefersToSameObject(handle, *it->second));
	auto result = std::move(it->second->buffer);
	buffer_map.erase(it);
	return result;
}

idx_t BufferedVMFReader::GetBufferIndex() {
	buffer_line_or_object_counts.push_back(-1);
	return buffer_index++;
}

void BufferedVMFReader::SetBufferLineOrObjectCount(VMFBufferHandle &handle, idx_t count) {
	lock_guard<mutex> guard(lock);
	D_ASSERT(buffer_map.find(handle.buffer_index) != buffer_map.end());
	D_ASSERT(RefersToSameObject(handle, *buffer_map.find(handle.buffer_index)->second));
	D_ASSERT(buffer_line_or_object_counts[handle.buffer_index] == -1);
	buffer_line_or_object_counts[handle.buffer_index] = count;
}

idx_t BufferedVMFReader::GetLineNumber(idx_t buf_index, idx_t line_or_object_in_buf) {
	D_ASSERT(options.format != VMFFormat::AUTO_DETECT);
	while (true) {
		idx_t line = line_or_object_in_buf;
		bool can_throw = true;
		{
			lock_guard<mutex> guard(lock);
			if (thrown) {
				return DConstants::INVALID_INDEX;
			}
			for (idx_t b_idx = 0; b_idx < buf_index; b_idx++) {
				if (buffer_line_or_object_counts[b_idx] == -1) {
					can_throw = false;
					break;
				} else {
					line += buffer_line_or_object_counts[b_idx];
				}
			}
			if (can_throw) {
				thrown = true;
				// SQL uses 1-based indexing so I guess we will do that in our exception here as well
				return line + 1;
			}
		}
		TaskScheduler::YieldThread();
	}
}

void BufferedVMFReader::ThrowParseError(idx_t buf_index, idx_t line_or_object_in_buf, yyjson_read_err &err,
                                         const string &extra) {
	string unit = options.format == VMFFormat::NEWLINE_DELIMITED ? "line" : "record/value";
	auto line = GetLineNumber(buf_index, line_or_object_in_buf);
	throw InvalidInputException("Malformed VMF in file \"%s\", at byte %llu in %s %llu: %s. %s", file_name,
	                            err.pos + 1, unit, line + 1, err.msg, extra);
}

void BufferedVMFReader::ThrowTransformError(idx_t buf_index, idx_t line_or_object_in_buf,
                                             const string &error_message) {
	string unit = options.format == VMFFormat::NEWLINE_DELIMITED ? "line" : "record/value";
	auto line = GetLineNumber(buf_index, line_or_object_in_buf);
	throw InvalidInputException("VMF transform error in file \"%s\", in %s %llu: %s", file_name, unit, line,
	                            error_message);
}

double BufferedVMFReader::GetProgress() const {
	lock_guard<mutex> guard(lock);
	if (HasFileHandle()) {
		return 100.0 - 100.0 * double(file_handle->Remaining()) / double(file_handle->FileSize());
	} else {
		return 0;
	}
}

} // namespace duckdb
