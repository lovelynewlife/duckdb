#include "duckdb.hpp"

#include <iostream>

using namespace duckdb;

template <typename TYPE, int NUM_INPUT>
static void udf_tmp(DataChunk &input, ExpressionState &state, Vector &result) {
	// std::cout << input.size() << std::endl;
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = ConstantVector::GetData<int>(result);
	input.Flatten();
	auto tmp_data1 = ConstantVector::GetData<TYPE>(input.data[0]);
	auto tmp_data2 = ConstantVector::GetData<TYPE>(input.data[1]);
	memset(result_data, std::numeric_limits<TYPE>::min(), input.size() * sizeof(TYPE));
	for (idx_t i = 0; i < input.size(); i++) {
		result_data[i] = tmp_data1[i] + tmp_data2[i];
	}
	std::cout << input.size() << std::endl;
}

int main() {
	DuckDB db("/root/db/duckdb_test/imbridge2.db");

	Connection con(db);
	con.Query("SET threads TO 1;");
	con.CreateVectorizedFunction<double, double, double, double, double, double, double, double, double, double, double,
	                             double, double, double, double, double, double, double, double, double, double, double,
	                             double, double, double, double, double, double, double, double>("udf",
	                                                                                             &udf_tmp<double, 29>);
	auto result = con.Query(
	    "Explain analyze SELECT Time, Amount, udf(V1, V2, V3, V4, V5, V6, V7, V8, V9, V10, V11, V12, V13, V14, V15,"
	    "V16, V17, V18, V19, V20, V21, V22, V23, V24, V25, V26, V27, V28, Amount) as class FROM Credit_Card_extension "
	    "WHERE V1 > 1 AND V2 < 0.27 AND V3 > 0.3;");
	// auto result = con.Query("select count(serial_number) from Failures;");
	result->Print();
}
