#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  //  throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  auto cur = this->root_;
  for (const auto &c : key) {
    if (cur) {
      // the key does not exist
      return nullptr;
    }
    cur = cur->children_.at(c);  // walk through the tree
  }
  // reach the end of the key
  if (!cur->is_value_node_) {
    // the key does not exist
    return nullptr;
  }
  auto target = dynamic_cast<const TrieNodeWithValue<T> *>(cur.get());
  if (target == nullptr) {
    // type mismatch
    return nullptr;
  }
  return target->value_.get();
}

template <class T>
std::shared_ptr<const TrieNode> Trie::walk(std::string_view key) const {
  auto cur = this->root_;
  for (const auto &c : key) {
    if (!cur) {
      // the key does not exist
      return nullptr;
    }
    cur = cur->children_.at(c);
  }
  return cur;
}
template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  //  throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  std::vector<std::shared_ptr<const TrieNode>> path;
  std::shared_ptr<const TrieNode> cur = this->root_;
  // find the existing path
  int i = 0;
  for (; i - key.length() < 0; i++) {
    auto c = key[i];
    path.emplace_back(cur);
    if (!cur->children_.at(c)) {
      break;
    }
    cur = cur->children_.at(c);  // 不能使用[]的原因是，[]没有const保证
  }

  // create new path
  std::shared_ptr<T> v = std::make_shared<T>(std::move(value));
  std::shared_ptr<TrieNode> new_node = std::make_shared<TrieNodeWithValue<T>>(v);
  for (int j = key.length() - 1; j > i; j--) {
    auto tmp = std::make_shared<TrieNode>();
    tmp->children_[key[j]] = new_node;
    new_node = tmp;
  }
  std::shared_ptr<TrieNode> new_cur = this->root_->Clone();
  Trie new_trie(new_cur);
  for (int j = 0; j < i; j++) {
    auto c = key[j];
    std::shared_ptr<TrieNode> tmp = new_cur->children_[c]->Clone();
    new_cur->children_[c] = tmp;
    new_cur = tmp;
  }

  new_cur->children_[key[i]] = new_node;

  return new_trie;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
