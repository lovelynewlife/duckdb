#include "duckdb/function/udf_function.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"

#include "duckdb/main/client_context.hpp"

namespace duckdb {

void UDFWrapper::RegisterFunction(string name, vector<LogicalType> args, LogicalType ret_type,
                                  scalar_function_t udf_function, ClientContext &context, LogicalType varargs,
								  FunctionKind kind, u_int32_t batch_size) {

	ScalarFunction scalar_function(std::move(name), std::move(args), std::move(ret_type), std::move(udf_function));
	scalar_function.varargs = std::move(varargs);
	scalar_function.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	scalar_function.bridge_info = make_shared_ptr<IMBridgeExtraInfo>(kind, batch_size);
	CreateScalarFunctionInfo info(scalar_function);
	info.schema = DEFAULT_SCHEMA;
	context.RegisterFunction(info);
}

void UDFWrapper::RegisterAggrFunction(AggregateFunction aggr_function, ClientContext &context, LogicalType varargs) {
	aggr_function.varargs = std::move(varargs);
	CreateAggregateFunctionInfo info(std::move(aggr_function));
	context.RegisterFunction(info);
}

} // namespace duckdb
