#include "duckdb.hpp"
#include "duckdb/common/arrow/arrow_transform_util.hpp"
#include "imlane/scheduler/scheduler.hpp"

#include <arrow/python/pyarrow.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

using namespace duckdb;
using namespace imbridge;

int main(int argc, char **argv) {

	if (argc < 2) {
		std::cout << "[Server] you shoule add a parameter\n";
		return 0;
	}
	std::string channel_name = argv[1];
	// std::cout << "[Server] start " << channel_name << " server\n";
	if (!Py_IsInitialized()) {
		Py_Initialize();
	}
	PyGILState_STATE gstate;
	gstate = PyGILState_Ensure();

	PyObject *dycacher = PyImport_ImportModule("dycacher");
	if (!dycacher) {
		PyErr_Print();
	}

	if (arrow::py::import_pyarrow()) {
		std::cout
		    << "[Server] import pyarrow error! make sure your default python environment has installed the pyarrow\n";
		exit(0);
	}

	imbridge::IMLaneScheduler scheduler;
	imbridge::SharedMemoryManager shm_server(channel_name, imbridge::ProcessKind::MANAGER);

	// prepare the environment
	std::ifstream file("/root/workspace/duckdb/examples/embedded-c++/imbridge/code.py");
	std::stringstream buffer;
	buffer << file.rdbuf();
	std::string python_code = buffer.str();
	PyRun_SimpleString(python_code.c_str());
	PyObject *main_module = PyImport_AddModule("__main__");
	PyObject *main_dict = PyModule_GetDict(main_module);
	PyObject *MyProcess = PyDict_GetItemString(main_dict, "MyProcess");
	PyObject *my_process_instance = PyObject_CallObject(MyProcess, NULL);
	if (my_process_instance == NULL) {
		PyErr_Print();
		return 0;
	}
	// std::cout << "[Server] prepare the environment   " << channel_name << std::endl;
	while (true) {
		try {
			scheduler.wait_task_queue();
			if (!scheduler.is_alive()) {
				break;
			}
		} catch (const std::exception &e) {
			std::cout<<"[Server exit]" << e.what() << std::endl;
			break;
		}
		// std::cout << "[Server] get ID\n";
		int id = scheduler.get_id_from_task_queue();

		imbridge::SharedMemoryManager shm(std::to_string(id), imbridge::ProcessKind::SERVER);

		// read table
		std::shared_ptr<arrow::Table> my_table = imbridge::ReadArrowTableFromSharedMemory(shm, INPUT_TABLE);

		// handle table and predict
		PyObject *py_table_tmp = arrow::py::wrap_table(std::move(my_table));
		PyObject *py_result = PyObject_CallMethod(my_process_instance, "process", "O", py_table_tmp);

		// get the result
		if (py_result != NULL) {
			my_table = arrow::py::unwrap_table(py_result).ValueOrDie();
		} else {
			PyErr_Print();
			return 0;
		}
		// std::cout << "[Server] handle finished\n";
		// write result to shared memory
		imbridge::WriteArrowTableToSharedMemory(my_table, shm, OUTPUT_TABLE);
		shm.sem_client->post();
		// TODO: update the avaliable queue
		shm.sem_server->wait();
		// std::cout << "[Server] end\n";
		scheduler.push_id_to_avaliable_queue(id);
	}
	// std::cout << "[Server] udf server " << channel_name << " closed\n";
	PyGILState_Release(gstate);
	Py_Finalize();
	return 0;
}