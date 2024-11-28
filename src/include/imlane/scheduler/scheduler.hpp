//===----------------------------------------------------------------------===//
//                         DuckDB
//
// imlane/scheduler/scheduler.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/common/arrow/arrow_transform_util.hpp"
#include "duckdb/main/client_context.hpp"

#include <boost/interprocess/containers/deque.hpp>
#include <boost/interprocess/ipc/message_queue.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/sync/interprocess_semaphore.hpp>
#include <iostream>
#include <thread>

namespace bi = boost::interprocess;

namespace duckdb {
namespace imbridge {

const std::string PRE_FIX = "imlane_";
const std::string GLOBAL_SHM_NAME = PRE_FIX + "global_shm";
const std::string TASK_QUEUE_NAME = PRE_FIX + "task_queue";
const std::string AVALIABLE_QUEUE_NAME = PRE_FIX + "avaliable_queue";
const std::string TASK_QUEUE_SHM_NAME = TASK_QUEUE_NAME + "_shm";
const std::string TASK_QUEUE_SHM_MTX = TASK_QUEUE_NAME + "_mtx";
// const std::string AVALIABLE_QUEUE_SHM_NAME = AVALIABLE_QUEUE_NAME + "_shm";
const std::string AVALIABLE_QUEUE_SHM_MTX = AVALIABLE_QUEUE_NAME + "_mtx";
const std::string SCHEDULER_ALIVE = PRE_FIX + "scheduler_alive";

const std::string START_SERVER_COMMAND = "/root/workspace/duckdb/examples/embedded-c++/imbridge/server_start.sh  ";
const int BASE_ID = 100000;

class IMLaneScheduler {
public:
	IMLaneScheduler(bool is_manager = false, const size_t size = 1024 * 1024)
	    : is_manager(is_manager), task_queue(bi::open_or_create, TASK_QUEUE_NAME.c_str(), 300, sizeof(int)),
	      avaliable_queue(bi::open_or_create, AVALIABLE_QUEUE_NAME.c_str(), 300, sizeof(int)) {
		global_segment = bi::managed_shared_memory(bi::open_or_create, GLOBAL_SHM_NAME.c_str(), size);
		task_queue_sem = global_segment.find_or_construct<bi::interprocess_semaphore>(TASK_QUEUE_SHM_NAME.c_str())(0);
		task_queue_mtx = global_segment.find_or_construct<bi::interprocess_mutex>(TASK_QUEUE_SHM_MTX.c_str())();
		avaliable_queue_mtx =
		    global_segment.find_or_construct<bi::interprocess_mutex>(AVALIABLE_QUEUE_SHM_MTX.c_str())();
		alive = global_segment.find_or_construct<bool>(SCHEDULER_ALIVE.c_str())(true);
		if (is_manager) {
			sys_cpu_core_nums = std::thread::hardware_concurrency();
		}
	}

	~IMLaneScheduler() {
		if (is_manager) {
			*alive = false;
			for (int i = 0; i < sys_cpu_core_nums; i++) {
				task_queue_sem->post();
			}
			// std::this_thread::sleep_for(std::chrono::seconds(3));
			destroy();
		}
	}

	void launch() {
		if (is_manager) {
			for (int i = 0; i < sys_cpu_core_nums; i++) {
				std::string cmd = START_SERVER_COMMAND + std::to_string(i + BASE_ID);
				std::thread t([cmd]() { std::system(cmd.c_str()); });
				t.detach();
				int msg = i + BASE_ID;
				avaliable_queue.send(&msg, sizeof(msg), 0);
			}
			std::cout << "[Client] launch server success" << std::endl;
			// std::this_thread::sleep_for(std::chrono::seconds(7));
		}
	}

	void destroy() {
		bi::shared_memory_object::remove(GLOBAL_SHM_NAME.c_str());
		bi::message_queue::remove(TASK_QUEUE_NAME.c_str());
		bi::message_queue::remove(AVALIABLE_QUEUE_NAME.c_str());
	}

	void schedule_udf(DataChunk &data, Vector &result, const ClientProperties &options) {
		auto table = imbridge::ConvertDataChunkToArrowTable(data, options);
		int id = get_id_from_avaliable_queue();
		if (id < BASE_ID) {
			std::cout << id << std::endl;
			throw std::runtime_error("id is not valid");
		}
		// write in shared_memory
		imbridge::SharedMemoryManager shm(std::to_string(id), imbridge::ProcessKind::CLIENT);
		imbridge::WriteArrowTableToSharedMemory(table, shm, imbridge::INPUT_TABLE);
		// schedule
		push_id_to_task_queue(id);
		shm.sem_client->wait();
		// read from shared_memory
		auto my_table = imbridge::ReadArrowTableFromSharedMemory(shm, imbridge::OUTPUT_TABLE);
		// int cols = my_table->num_columns(), rows = my_table->num_rows();
		// std::vector<int64_t> res;
		// for (int i = 0; i < my_table->num_columns(); i++) {
		// 	std::shared_ptr<arrow::ChunkedArray> column = my_table->column(i);
		// 	for (int chunk_idx = 0; chunk_idx < column->num_chunks(); chunk_idx++) {
		// 		auto chunk = std::static_pointer_cast<arrow::Int64Array>(column->chunk(chunk_idx));
		// 		for (int j = 0; j < chunk->length(); j++) {
		// 			res.push_back(chunk->Value(j));
		// 		}
		// 	}
		// }
		imbridge::ConvertArrowTableResultToVector(my_table, result);
		shm.destroy_shared_memory_object<char>(INPUT_TABLE);
		shm.destroy_shared_memory_object<char>(OUTPUT_TABLE);
		shm.sem_server->post();
	}

	void wait_task_queue() {
		task_queue_sem->wait();
	}

	bool is_alive() {
		return *alive;
	}

	// client : write data to lane
	int get_id_from_avaliable_queue() {
		int id;
		bi::message_queue::size_type recvd_size;
		unsigned int priority;
		avaliable_queue.receive(&id, sizeof(id), recvd_size, priority);
		return id;
	}

	// server : avalaible for client
	void push_id_to_avaliable_queue(int id) {
		avaliable_queue.send(&id, sizeof(id), 0);
	}

	// server : read data from lane
	int get_id_from_task_queue() {
		int id;
		bi::message_queue::size_type recvd_size;
		unsigned int priority;
		task_queue.receive(&id, sizeof(id), recvd_size, priority);
		return id;
	}

	// slient : schedule id
	void push_id_to_task_queue(int id) {
		task_queue.send(&id, sizeof(id), 0);
		task_queue_sem->post();
	}

private:
	int sys_cpu_core_nums = 0;
	bi::managed_shared_memory global_segment;
	bi::message_queue task_queue;
	bi::message_queue avaliable_queue;
	bi::interprocess_semaphore *task_queue_sem;
	bi::interprocess_mutex *task_queue_mtx;
	bi::interprocess_mutex *avaliable_queue_mtx;
	bool *alive;
	bool is_manager;
};

} // namespace imbridge
} // namespace duckdb