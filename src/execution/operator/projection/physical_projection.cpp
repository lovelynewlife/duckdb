#include "duckdb/execution/operator/projection/physical_projection.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "imbridge/execution/batch_controller.hpp"

#include <future>
#include <iostream>

namespace duckdb {

class ProjectionState : public OperatorState {
public:
	explicit ProjectionState(ExecutionContext &context, const vector<unique_ptr<Expression>> &expressions)
	    : executor(context.client, expressions) {
	}

	explicit ProjectionState(ExecutionContext &context, const vector<unique_ptr<Expression>> &expressions,
	                         const vector<LogicalType> &input_types, idx_t buffer_capacity = DEFAULT_RESERVED_CAPACITY)
	    : executor(context.client, expressions) {
		controller = make_uniq<imbridge::BatchController>();
		controller->Initialize(Allocator::Get(context.client), input_types, buffer_capacity);
	}

	ExpressionExecutor executor;
	unique_ptr<imbridge::BatchController> controller;
	std::vector<std::future<unique_ptr<DataChunk>>> res_collect;

public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op, executor, "projection", 0);
	}
};

PhysicalProjection::PhysicalProjection(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
                                       idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::PROJECTION, std::move(types), estimated_cardinality),
      select_list(std::move(select_list)) {
}

OperatorResultType PhysicalProjection::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                               GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = state_p.Cast<ProjectionState>();
	auto &controller = state.controller;
	auto &res_collect = state.res_collect;
	
	for (auto it = res_collect.begin(); it != res_collect.end(); ++it) {
		if (it->wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
			unique_ptr<DataChunk> out_buffer = it->get();
			vector<int64_t> check;
			out_buffer->Copy(chunk);
			auto tmp_data1 = ConstantVector::GetData<int64_t>(chunk.data[1]);
			for (auto i = 0; i < out_buffer->size(); ++i) {
				check.push_back(tmp_data1[i]);
			}
			res_collect.erase(it);
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}
	}

	unique_ptr<DataChunk> in_buffer = make_uniq<DataChunk>();
	in_buffer->Initialize(Allocator::Get(context.client), input.GetTypes(), STANDARD_VECTOR_SIZE);
	input.Copy(*in_buffer);
	res_collect.push_back(
	    std::async(std::launch::async, [&context, &state, in_buffer = std::move(in_buffer), &chunk]() mutable {
		    return state.controller->AsyncExecuteExpression(context.client, state.executor, std::move(in_buffer),
		                                                    chunk.GetTypes());
	    }));

	return OperatorResultType::NEED_MORE_INPUT;
}

unique_ptr<OperatorState> PhysicalProjection::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<ProjectionState>(context, select_list, children[0]->GetTypes());
}

unique_ptr<PhysicalOperator>
PhysicalProjection::CreateJoinProjection(vector<LogicalType> proj_types, const vector<LogicalType> &lhs_types,
                                         const vector<LogicalType> &rhs_types, const vector<idx_t> &left_projection_map,
                                         const vector<idx_t> &right_projection_map, const idx_t estimated_cardinality) {

	vector<unique_ptr<Expression>> proj_selects;
	proj_selects.reserve(proj_types.size());

	if (left_projection_map.empty()) {
		for (storage_t i = 0; i < lhs_types.size(); ++i) {
			proj_selects.emplace_back(make_uniq<BoundReferenceExpression>(lhs_types[i], i));
		}
	} else {
		for (auto i : left_projection_map) {
			proj_selects.emplace_back(make_uniq<BoundReferenceExpression>(lhs_types[i], i));
		}
	}
	const auto left_cols = lhs_types.size();

	if (right_projection_map.empty()) {
		for (storage_t i = 0; i < rhs_types.size(); ++i) {
			proj_selects.emplace_back(make_uniq<BoundReferenceExpression>(rhs_types[i], left_cols + i));
		}

	} else {
		for (auto i : right_projection_map) {
			proj_selects.emplace_back(make_uniq<BoundReferenceExpression>(rhs_types[i], left_cols + i));
		}
	}

	return make_uniq<PhysicalProjection>(std::move(proj_types), std::move(proj_selects), estimated_cardinality);
}

string PhysicalProjection::ParamsToString() const {
	string extra_info;
	for (auto &expr : select_list) {
		extra_info += expr->GetName() + "\n";
	}
	return extra_info;
}

OperatorFinalizeResultType PhysicalProjection::FinalExecute(ExecutionContext &context, DataChunk &chunk,
                                                            GlobalOperatorState &gstate, OperatorState &state) const {
	auto &state_p = state.Cast<ProjectionState>();
	auto &res_collect = state_p.res_collect;

	for (auto it = res_collect.begin(); it != res_collect.end(); ++it) {
		if (it->wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
			unique_ptr<DataChunk> out_buffer = it->get();
			out_buffer->Copy(chunk);
			res_collect.erase(it);
			return OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
		}
	}
	if(res_collect.empty()) {
		return OperatorFinalizeResultType::FINISHED;
	} else {
		return OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
	}

}

} // namespace duckdb
