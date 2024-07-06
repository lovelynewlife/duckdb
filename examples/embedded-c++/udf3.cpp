#include "duckdb.hpp"

#include <iostream>

using namespace duckdb;

template <typename TYPE, int NUM_INPUT>
static void udf_tmp(DataChunk &input, ExpressionState &state, Vector &result) {
	std::cout << input.size() << std::endl;
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = ConstantVector::GetData<int>(result);
	input.Flatten();
	auto tmp_data1 = ConstantVector::GetData<TYPE>(input.data[0]);
	auto tmp_data2 = ConstantVector::GetData<TYPE>(input.data[1]);
	// auto tmp_data3 = ConstantVector::GetData<TYPE>(input.data[2]);
	// auto tmp_data4 = ConstantVector::GetData<TYPE>(input.data[3]);
	// auto tmp_data5 = ConstantVector::GetData<TYPE>(input.data[4]);
	// auto tmp_data6 = ConstantVector::GetData<TYPE>(input.data[5]);
	// auto tmp_data7 = ConstantVector::GetData<TYPE>(input.data[6]);
	memset(result_data, std::numeric_limits<TYPE>::min(), input.size() * sizeof(TYPE));
	for (idx_t i = 0; i < input.size(); i++) {
		result_data[i] = tmp_data1[i] + tmp_data2[i];
	}
}

int main() {
	DuckDB db("/root/duckdb_test/imbridge.db");

	Connection con(db);
	con.Query("SET threads TO 1;");
	con.CreateVectorizedFunction<double, int64_t, int64_t>("udf", &udf_tmp<int64_t, 2>);
	auto result = con.Query(
	    "explain analyze select userID, productID, r, score  from (select userID, productID, score, rank() OVER "
	    "(PARTITION BY userID ORDER BY score) as r  from (select userID, productID, udf(userID, productID) score  from "
	    "(select userID, productID  from Product_Rating group by userID, productID))) where r <=10;");
	// auto result = con.Query("select count(serial_number) from Failures;");
	result->Print();
}
