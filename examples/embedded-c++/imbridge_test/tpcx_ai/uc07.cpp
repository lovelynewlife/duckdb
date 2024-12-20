
#include "duckdb.hpp"

#include <chrono>
#include <iostream>
#include <string>
#include <thread>

using namespace duckdb;
using namespace imbridge;

static void udf_tmp(DataChunk &input, ExpressionState &state, Vector &result) {
	using TYPE = double;
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = ConstantVector::GetData<TYPE>(result);
	input.Flatten();
	auto tmp_data1 = ConstantVector::GetData<TYPE>(input.data[0]);
	auto tmp_data2 = ConstantVector::GetData<TYPE>(input.data[1]);
	memset(result_data, std::numeric_limits<int64_t>::min(), input.size() * sizeof(int64_t));
	for (idx_t i = 0; i < input.size(); i++) {
		result_data[i] = (int)tmp_data1[i];
	}
}

int main() {
	DuckDB db("/root/workspace/duckdb/examples/embedded-c++/imbridge_test/db/db_tpcx_ai_sf40.db");
	Connection con(db);
	con.CreateVectorizedFunction<double, int64_t, int64_t>("udf", &udf_tmp, LogicalType::INVALID,
	                                                      FunctionKind::ASYNC_PREDICTION, 4096);

	string sql = R"(
explain analyze select userID, productID, r, score  from (select userID, productID, score, rank() OVER (PARTITION BY userID ORDER BY score) as r  from (select userID, productID, udf(userID, productID) score  from (select userID, productID  from Product_Rating group by userID, productID))) where r <=10;
)";
	int times = 5;
	double result = 0;
	double min1, max1;
	bool flag = true;
	for (int i = 0; i < times; i++) {
		auto start_time = std::chrono::high_resolution_clock::now();
		con.Query(sql);
		auto end_time = std::chrono::high_resolution_clock::now();
		auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
		double t = duration / 1e6;
		printf("%d : %lf\n", i+1, t);
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
	printf("finished execute %lf s!\n", result / (times * 1.0));
	return 0;
}