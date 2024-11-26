//===----------------------------------------------------------------------===//
//                         DuckDB
//
// imlane/scheduler/scheduler.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/arrow/arrow_transform_util.hpp"

#include <boost/interprocess/containers/deque.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/sync/interprocess_semaphore.hpp>
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

typedef bi::allocator<int, bi::managed_shared_memory::segment_manager> ShmemAllocator;
typedef bi::deque<int, ShmemAllocator> SharedDeque;

class IMLaneScheduler {
public:
	IMLaneScheduler(bool is_manager = false, const size_t size = 1024 * 1024) : is_manager(is_manager) {
		global_segment = bi::managed_shared_memory(bi::open_or_create, GLOBAL_SHM_NAME.c_str(), size);
		task_queue = global_segment.find_or_construct<SharedDeque>(TASK_QUEUE_NAME.c_str())(
		    global_segment.get_segment_manager());
		avaliable_queue = global_segment.find_or_construct<SharedDeque>(AVALIABLE_QUEUE_NAME.c_str())(
		    global_segment.get_segment_manager());
		task_queue_sem = global_segment.find_or_construct<bi::interprocess_semaphore>(TASK_QUEUE_SHM_NAME.c_str())(0);
		// avaliable_queue_sem =
		//     global_segment.find_or_construct<bi::interprocess_semaphore>(AVALIABLE_QUEUE_SHM_NAME.c_str())(0);
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
            for(int i =0;i<sys_cpu_core_nums;i++){
                task_queue_sem->post();
            }
            std::this_thread::sleep_for(std::chrono::seconds(3));
			destroy();
		}
	}

	void launch() {
		if (is_manager) {
			for (int i = 0; i < sys_cpu_core_nums; i++) {
				std::string cmd = START_SERVER_COMMAND + std::to_string(i);
				std::thread t([cmd]() { std::system(cmd.c_str()); });
				t.detach();
				avaliable_queue->push_back(i);
			}
		}
	}

	void destroy() {
		bi::shared_memory_object::remove(GLOBAL_SHM_NAME.c_str());
	}

	void schedule_udf(DataChunk &data, Vector &result, const ClientProperties &options) {
		auto table = imbridge::ConvertDataChunkToArrowTable(data, options);

        int id = get_id_from_avaliable_queue();

         // write in shared_memory
        imbridge::SharedMemoryManager shm(std::to_string(id), imbridge::ProcessKind::CLIENT);
        imbridge::WriteArrowTableToSharedMemory(table, shm, imbridge::INPUT_TABLE);

        // schedule
        push_id_to_task_queue(id);
        shm.sem_client->wait();

        // read from shared_memory
        auto my_table = imbridge::ReadArrowTableFromSharedMemory(shm, imbridge::OUTPUT_TABLE);

        shm.destroy_shared_memory_object<char>(imbridge::INPUT_TABLE);
		shm.destroy_shared_memory_object<char>(imbridge::OUTPUT_TABLE);
        int cols = my_table->num_columns(), rows = my_table->num_rows();
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
        shm.sem_server->post();
	}

	void wait_task_queue(){
		task_queue_sem->wait();
	}

	bool is_alive(){
		return *alive;
	}

	// client : write data to lane
	int get_id_from_avaliable_queue() {
		int id;
		{
			bi::scoped_lock<bi::interprocess_mutex> lock(*avaliable_queue_mtx);
			id = avaliable_queue->front();
			avaliable_queue->pop_front();
		}
		return id;
	}

	// server : avalaible for client
	void push_id_to_avaliable_queue(int id) {
		{
			bi::scoped_lock<bi::interprocess_mutex> lock(*avaliable_queue_mtx);
			avaliable_queue->push_back(id);
		}
	}

    // server : read data from lane
	int get_id_from_task_queue() {
		int id;
		{
			bi::scoped_lock<bi::interprocess_mutex> lock(*task_queue_mtx);
			id = task_queue->front();
			task_queue->pop_front();
		}
		return id;
	}

    // slient : schedule id
	void push_id_to_task_queue(int id) {
		{
			bi::scoped_lock<bi::interprocess_mutex> lock(*task_queue_mtx);
			task_queue->push_back(id);
		}
        task_queue_sem->post();
	}

private:
	int sys_cpu_core_nums = 0;
	bi::managed_shared_memory global_segment;
	SharedDeque *task_queue;
	SharedDeque *avaliable_queue;
	bi::interprocess_semaphore *task_queue_sem;
	// bi::interprocess_semaphore *avaliable_queue_sem;
	bi::interprocess_mutex *task_queue_mtx;
	bi::interprocess_mutex *avaliable_queue_mtx;
    bool *alive;
	bool is_manager;

	
};

} // namespace imbridge
} // namespace duckdb