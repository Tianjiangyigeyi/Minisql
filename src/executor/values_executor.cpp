//
// Created by njz on 2023/1/31.
//

#include "executor/executors/values_executor.h"

ValuesExecutor::ValuesExecutor(ExecuteContext *exec_ctx, const ValuesPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void ValuesExecutor::Init() {
  value_size_ = plan_->GetValues().size();
}

bool ValuesExecutor::Next(Row *row, RowId *rid) {
  if (cursor_ < value_size_) {
    // LOG(INFO) << "ksergj";
    std::vector<Field> values;
    // LOG(INFO) << "fgjksergcfgjj";
    auto exprs = plan_->GetValues().at(cursor_);
    // LOG(INFO) << "fgjssksergj";
    for (auto expr : exprs) {
      values.emplace_back(expr->Evaluate(nullptr));
    }
    LOG(INFO) << "12fgjssksergj";
    // LOG(INFO) << "values size " << values.size();
    *row = Row{values};
    LOG(INFO) << "1jdrtj2fgjssksergj";
    cursor_++;
    LOG(INFO) << "4fgjssksergj";

    return true;
  }
  return false;
}
