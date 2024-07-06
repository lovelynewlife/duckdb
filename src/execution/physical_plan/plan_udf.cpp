#include "duckdb/execution/operator/udf/physical_udf.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_udf.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalUDF &op) {
	D_ASSERT(op.children.size() == 1);
	auto plan = CreatePlan(*op.children[0]);

	auto udf = make_uniq<PhysicalUDF>(op.types, op.estimated_cardinality);
	udf->children.push_back(std::move(plan));
	return std::move(udf);
}

} // namespace duckdb
