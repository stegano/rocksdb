#pragma once

int compareRev(const rocksdb::Slice& a, const rocksdb::Slice& b) {
  auto indexA = 0UL;
  auto indexB = 0UL;
  const auto endA = a.size();
  const auto endB = b.size();

  // Compare the revision number
  auto result = 0;
  const auto end = std::min(endA, endB);
  while (indexA < end && indexB < end) {
    const auto ac = a[indexA++];
    const auto bc = b[indexB++];

    if (ac == '-') {
      if (bc == '-') {
        break;
      }
      return -1;
    } else if (bc == '-') {
      return 1;
    }

    if (!result) {
      result = ac == bc ? 0 : ac < bc ? -1 : 1;
    }
  }

  if (result) {
    return result;
  }

  // Compare the rest
  while (indexA < end && indexB < end) {
    const auto ac = a[indexA++];
    const auto bc = b[indexB++];
    if (ac != bc) {
      return ac < bc ? -1 : 1;
    }
  }

  return endA - endB;
}

class MaxRevOperator : public rocksdb::MergeOperator {
 public:
  bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override {
    rocksdb::Slice& max = merge_out->existing_operand;
    if (merge_in.existing_value) {
      max = rocksdb::Slice(merge_in.existing_value->data(),
                  merge_in.existing_value->size());
    } else if (max.data() == nullptr) {
      max = rocksdb::Slice();
    }

    for (const auto& op : merge_in.operand_list) {
      if (compareRev(max, op) < 0) {
        max = op;
      }
    }

    return true;
  }

  bool PartialMerge(const rocksdb::Slice& /*key*/, const rocksdb::Slice& left_operand,
                    const rocksdb::Slice& right_operand, std::string* new_value,
                    rocksdb::Logger* /*logger*/) const override {
    if (compareRev(left_operand, right_operand) >= 0) {
      new_value->assign(left_operand.data(), left_operand.size());
    } else {
      new_value->assign(right_operand.data(), right_operand.size());
    }
    return true;
  }

  bool PartialMergeMulti(const rocksdb::Slice& /*key*/,
                         const std::deque<rocksdb::Slice>& operand_list,
                         std::string* new_value,
                         rocksdb::Logger* /*logger*/) const override {
    rocksdb::Slice max;
    for (const auto& operand : operand_list) {
      if (compareRev(max, operand) < 0) {
        max = operand;
      }
    }

    new_value->assign(max.data(), max.size());
    return true;
  }

  static const char* kClassName() { return "MaxRevOperator"; }
  static const char* kNickName() { return "maxRev"; }
  const char* Name() const override { return kClassName(); }
  const char* NickName() const override { return kNickName(); }
};