
#include "duckdb.hpp"

#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <sstream>
#include <fstream>

using namespace duckdb;
using namespace imbridge;

static void udf_tmp(DataChunk &input, ExpressionState &state, Vector &result) {
	using TYPE = double;
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = ConstantVector::GetData<TYPE>(result);
	input.Flatten();
	auto tmp_data1 = ConstantVector::GetData<TYPE>(input.data[0]);
	auto tmp_data2 = ConstantVector::GetData<TYPE>(input.data[1]);
	memset(result_data, std::numeric_limits<TYPE>::min(), input.size() * sizeof(TYPE));
	for (idx_t i = 0; i < input.size(); i++) {
		result_data[i] = 1 * tmp_data1[i] + 1 * tmp_data2[i];
	}
}

void bs_process(Connection &con, int dbt, int mlt, int max_bs = 6200, int now_bs = 256, int base_bs = 256) {
	while (max_bs > now_bs) {
		std::stringstream ss;
		ss << "udf" << now_bs;
		std::string udf_name = ss.str();
		con.CreateVectorizedFunction<int64_t, double, double, double, double, double, double, double, int64_t, double,
		                             int64_t, string_t, string_t, int64_t, int64_t, int64_t, int64_t, int64_t, int64_t,
		                             int64_t, int64_t, int64_t, int64_t, int64_t>(
		    udf_name, &udf_tmp, LogicalType::INVALID, FunctionKind::PREDICTION, now_bs);

		ss.str("");
		ss.clear();
		ss << R"(
Explain analyze SELECT eid, )"
		   << udf_name << R"((hematocrit, neutrophils, sodium, glucose, bloodureanitro, creatinine, bmi, pulse,
 respiration, secondarydiagnosisnonicd9, rcount, gender, cast(dialysisrenalendstage as INTEGER), cast(asthma as INTEGER),
  cast(irondef as INTEGER), cast(pneum as INTEGER), cast(substancedependence as INTEGER),
   cast(psychologicaldisordermajor as INTEGER), cast(depress as INTEGER), cast(psychother as INTEGER),
    cast(fibrosisandother as INTEGER), cast(malnutrition as INTEGER), cast(hemo as INTEGER)) AS lengthofstay
   FROM LengthOfStay_extension WHERE hematocrit > 10 AND neutrophils > 10 AND bloodureanitro < 20 AND pulse < 70;
)";
		int times = 5;
		double result = 0;
		double min1, max1;
		bool flag = true;
		for (int i = 0; i < times; i++) {
			auto start_time = std::chrono::high_resolution_clock::now();
			con.Query(ss.str());
			auto end_time = std::chrono::high_resolution_clock::now();
			auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
			double t = duration / 1e6;
			printf("%d : %lf\n", i + 1, t);
			result += t;
			if (flag) {
				flag = false;
				min1 = t;
				max1 = t;
			} else {
				min1 = std::min(min1, t);
				max1 = std::max(max1, t);
			}
		}
		printf("min : %lf\n", min1);
		printf("max : %lf\n", max1);
		result = result - min1 - max1;
		times = times - 2;
		printf("db threads: %d, ml threads: %d, bs: %d, finished execute %lf s!\n", dbt, mlt, now_bs,
		       result / (times * 1.0));
		now_bs += base_bs;
	}
}

int main() {

	// set sys threads
	int threads;
	std::ifstream file1("/root/workspace/duckdb/.vscode/experiment_cfg/sys_thread.cfg");
	if (file1.is_open()) {
		file1 >> threads;
		file1.close();
	} else {
		std::cout << "count not open ml file!" << std::endl;
		exit(0);
	}

	for (int i = 1; i <= threads; i++) {
		// set ml threads
		std::ofstream file("/root/workspace/duckdb/.vscode/experiment_cfg/ml_thread.cfg");
		if (file.is_open()) {
			file << i;
			file.close();
		} else {
			std::cout << "count not open ml file!" << std::endl;
			exit(0);
		}
		DuckDB db("/root/workspace/duckdb/examples/embedded-c++/imbridge_test/db/db_raven_10G.db");
		Connection con(db);
		for (int j = 1; j <= threads; j++) {
			std::stringstream ss;
			// set db threads
			ss << "SET threads TO " << j << ";";
			con.Query(ss.str());
			// set udf
			bs_process(con, j, i);
		}
	}
	return 0;
}