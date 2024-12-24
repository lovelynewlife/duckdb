#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

namespace imbridge {

class PhysicalPredictionFilter : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::PREDICTION_FILTER;

public:
	PhysicalPredictionFilter(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
     idx_t estimated_cardinality, idx_t prediction_size);

	//! The filter expression
	unique_ptr<Expression> expression;
    idx_t user_defined_size;
	bool use_adaptive_size;

public:
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

	bool ParallelOperator() const override {
		return true;
	}

	bool RequiresFinalExecute() const {
		return true;
	}

	OperatorFinalizeResultType FinalExecute(ExecutionContext &context, 
	DataChunk &chunk, GlobalOperatorState &gstate, OperatorState &state) const final;

	string ParamsToString() const override;

protected:
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                   GlobalOperatorState &gstate, OperatorState &state) const override;
};

} // namespace imbridge

} // namespace duckdb