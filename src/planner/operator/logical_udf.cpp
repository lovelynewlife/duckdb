#include "duckdb/planner/operator/logical_udf.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

LogicalUDF::LogicalUDF(unique_ptr<Expression> expression) : LogicalOperator(LogicalOperatorType::LOGICAL_UDF) {
	expressions.push_back(std::move(expression));
}

LogicalUDF::LogicalUDF() : LogicalOperator(LogicalOperatorType::LOGICAL_UDF) {
}

void LogicalUDF::ResolveTypes() {
	types = children[0]->types;
}

vector<ColumnBinding> LogicalUDF::GetColumnBindings() {
	return children[0]->GetColumnBindings();
}


} // namespace duckdb
