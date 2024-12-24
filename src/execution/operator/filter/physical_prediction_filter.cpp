#include "duckdb/execution/physical_operator.hpp"
#include "imbridge/execution/operator/physical_prediction_filter.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/parallel/thread_context.hpp"

namespace duckdb {
namespace imbridge {

class PredictionFilterState: public PredictionState {
public:
	explicit PredictionFilterState(ExecutionContext &context, Expression &expr,
    const vector<LogicalType> &input_types, idx_t prediction_size = INITIAL_PREDICTION_SIZE, bool adaptive = false, idx_t buffer_capacity = DEFAULT_RESERVED_CAPACITY)
	    : PredictionState(context, input_types, expr, adaptive, prediction_size, buffer_capacity), executor(context.client, expr, buffer_capacity) {
	}

	ExpressionExecutor executor;
	SelectionVector sel;

public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op, executor, "prediction_filter", 0);
	}
};

PhysicalPredictionFilter::PhysicalPredictionFilter(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
 idx_t estimated_cardinality, idx_t user_defined_size): PhysicalOperator(PhysicalOperatorType::PREDICTION_FILTER, std::move(types), estimated_cardinality) {
	D_ASSERT(select_list.size() > 0);
	if (select_list.size() > 1) {
		// create a big AND out of the expressions
		auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
		for (auto &expr : select_list) {
			conjunction->children.push_back(std::move(expr));
		}
		expression = std::move(conjunction);
	} else {
		expression = std::move(select_list[0]);
	}

    if(user_defined_size <= 0) {
        this->user_defined_size = INITIAL_PREDICTION_SIZE;
        use_adaptive_size = true; 
    } else {
        this->user_defined_size = user_defined_size;
        use_adaptive_size = false;
    }
}

unique_ptr<OperatorState> PhysicalPredictionFilter::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<PredictionFilterState>(context, *expression, children[0]->GetTypes(), user_defined_size, use_adaptive_size);
}

OperatorResultType PhysicalPredictionFilter::Execute(ExecutionContext &context, DataChunk &input,
 DataChunk &chunk, GlobalOperatorState &gstate, OperatorState &state) const {
    auto &state_p = state.Cast<PredictionFilterState>();
    auto &controller = state_p.controller;
    // auto &out_buf = state_p.output_buffer;
    auto &sel = state_p.sel;
    auto &padded = state_p.padded;
    auto &output_left = state_p.output_left;
    auto &base_offset = state_p.base_offset;
    idx_t &batch_size = state_p.prediction_size;

    auto ret = OperatorResultType::HAVE_MORE_OUTPUT;

    // batch adapting
    if (output_left) {
        if (output_left <= STANDARD_VECTOR_SIZE) {
            // controller->BatchAdapting(*out_buf, chunk, base_offset, output_left);
            output_left = 0;
            base_offset = 0;
        } else {
            // controller->BatchAdapting(*out_buf, chunk, base_offset);
            output_left -= STANDARD_VECTOR_SIZE;
            base_offset += STANDARD_VECTOR_SIZE;
        }

        return ret;
    }

    switch (controller->GetState()) {
        case BatchControllerState::SLICING: {
            batch_size = state_p.tuner.GetBatchSize();
            if (controller->HasNext(batch_size)) {
                // NEXT_EXE_ADAPT(state, controller, batch_size, out_buf, chunk,
                // OperatorResultType::HAVE_MORE_OUTPUT, OperatorResultType::HAVE_MORE_OUTPUT, ret);
            } else {
                // check wheather the buffer should be reset
                if (controller->GetSize() == 0) {
                    // the buffer state is reset to EMPTY
                    controller->ResetBuffer();
                } else {
                    controller->SetState(BatchControllerState::BUFFERRING);
                }
                ret = OperatorResultType::NEED_MORE_INPUT;
            }            
            break;
        }
        case BatchControllerState::EMPTY: {
            batch_size = state_p.tuner.GetBatchSize();
            controller->ResetBuffer();
            idx_t remained = input.size() - padded;
            ret = OperatorResultType::NEED_MORE_INPUT;

            if (remained > 0) {
                controller->PushChunk(input, padded, input.size());
                if (remained < batch_size) {
                    controller->SetState(BatchControllerState::BUFFERRING); 
                } else {
                    // opt: perform slicing directly
                    controller->SetState(BatchControllerState::SLICING);
                    ret = OperatorResultType::HAVE_MORE_OUTPUT;
                }
            }
            padded = 0;
            break;
        }
        case BatchControllerState::BUFFERRING: {
            batch_size = state_p.tuner.GetBatchSize();

            if (controller->GetSize() + input.size() < batch_size) {
                controller->PushChunk(input);
                controller->SetState(BatchControllerState::BUFFERRING);
                ret = OperatorResultType::NEED_MORE_INPUT;
            } else {
                padded = batch_size - controller->GetSize();
                controller->PushChunk(input, 0, padded);

                // NEXT_EXE_ADAPT(state, controller, batch_size, out_buf, chunk, 
                // OperatorResultType::HAVE_MORE_OUTPUT, OperatorResultType::HAVE_MORE_OUTPUT, ret);

                controller->SetState(BatchControllerState::EMPTY);
            }  
            break;
        }

        default:
            throw InternalException("BatchController State Unsupported");  
    }

    idx_t result_count = state_p.executor.SelectExpression(input, state_p.sel);
    if (result_count == input.size()) {
        // nothing was filtered: skip adding any selection vectors
        chunk.Reference(input);
    } else {
        chunk.Slice(input, state_p.sel, result_count);
    }

    return ret;
}

OperatorFinalizeResultType PhysicalPredictionFilter::FinalExecute(ExecutionContext &context,
 DataChunk &chunk, GlobalOperatorState &gstate, OperatorState &state) const {
    return OperatorFinalizeResultType::FINISHED;
 }

string PhysicalPredictionFilter::ParamsToString() const {
    auto result = expression->GetName();
    result += "\n[INFOSEPARATOR]\n";
    result += StringUtil::Format("EC: %llu", estimated_cardinality);
    if(!use_adaptive_size){
        result += StringUtil::Format("\nprediction size: %llu\n", user_defined_size);
    } else {
        result += "\nprediction size: adaptive\n";
    }
    return result;
}

} // namespace imbridge
    

} // namespace duckdb
