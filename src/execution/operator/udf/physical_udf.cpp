#include "duckdb/execution/operator/udf/physical_udf.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

class UDFState : public OperatorState {
public:
	explicit UDFState(ExecutionContext &context) : executor(context.client) {
	}

	ExpressionExecutor executor;
	idx_t DIBS = 4096;
	bool is_init_current_chunk = false;
	bool has_buffer = true;
	DataChunk current_chunk;

public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op, executor, "udf", 0);
	}
};

PhysicalUDF::PhysicalUDF(vector<LogicalType> types, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::UDF, std::move(types), estimated_cardinality) {
}

OperatorResultType PhysicalUDF::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                        GlobalOperatorState &gstate, OperatorState &state_p) const {
	idx_t count = input.size();
	auto &state = state_p.Cast<UDFState>();
	auto &client_context = context.client;

	if (count == 0 && !client_context.zero_pipeline) {
		return OperatorResultType::NEED_MORE_INPUT;
	}
	// init current_chunk
	if (!state.is_init_current_chunk) {
		state.current_chunk.Initialize(client_context, input.GetTypes());
		client_context.udf_count = MaxValue<idx_t>(client_context.udf_count + 1, 1);
		state.is_init_current_chunk = true;
	}

	// merge the batch data_chunk in save_chunk
	if (count > 0) {
		idx_t now_count = count + state.current_chunk.size();
		if (now_count < state.DIBS) {
			if (client_context.zero_pipeline) {
				chunk.Append(state.current_chunk, true);
				chunk.Append(input, true);
				client_context.udf_count -= 1;
			} else {
				state.current_chunk.Append(input, true);
			}
		} else {
			// splite save_chunk to fit the desirable inference batch size
			idx_t diff = state.DIBS - state.current_chunk.size();
			chunk.Append(state.current_chunk, true);
			SelectionVector tmp1(0, diff);
			chunk.Append(input, true, &tmp1, diff);
			state.current_chunk.Reset();

			// if left the data in input, save it to current_chunk
			if (now_count > state.DIBS) {
				SelectionVector tmp2(diff, count - diff);
				state.current_chunk.Append(input, true, &tmp2, count - diff);
			}
		}
	} else {
		// the count == 0 means no data to scan
		chunk.Reference(state.current_chunk);
		if (client_context.zero_pipeline) {
			client_context.udf_count -= 1;
		}
	}
	return OperatorResultType::NEED_MORE_INPUT;
}

unique_ptr<OperatorState> PhysicalUDF::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<UDFState>(context);
}

string PhysicalUDF::ParamsToString() const {
	string extra_info;
	extra_info += "233 \n";
	return extra_info;
}

} // namespace duckdb
