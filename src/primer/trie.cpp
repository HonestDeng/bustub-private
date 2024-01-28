#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  //  throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  if (!this->root_) {
    // if the root node is nullptr
    return nullptr;
  }
  auto cur = this->root_;
  for (const auto &c : key) {
    if (!cur) {
      // the key does not exist
      return nullptr;
    }
    if (cur->children_.count(c) == 0) {
      return nullptr;
    }
    cur = cur->children_.at(c);  // walk through the tree
  }
  // reach the end of the key
  if (!cur->is_value_node_) {
    // the key does not exist
    return nullptr;
  }
  auto target = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(cur);
  if (target == nullptr) {
    // type mismatch
    return nullptr;
  }
  return target->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  //  throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  // the key is empty
  if (key.empty()) {
    std::shared_ptr<TrieNode> new_root = root_->Clone();
    new_root = std::make_shared<TrieNodeWithValue<T>>(new_root->children_, std::make_shared<T>(std::move(value)));
    return Trie(new_root);
  }

  std::shared_ptr<TrieNode> cur;
  if (this->root_) {
    cur = this->root_->Clone();
  } else {
    cur = std::make_shared<TrieNode>();
  }
  Trie new_trie(cur);
  int len = static_cast<int>(key.size());
  for (int i = 0; i < len; i++) {
    auto c = key[i];
    if (!cur->children_[c]) {
      // 如果子节点不存在
      std::shared_ptr<TrieNode> tmp = std::make_shared<TrieNode>();
      if (i - key.length() + 1 == 0) {
        tmp = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
      }
      cur->children_[c] = tmp;
      cur = tmp;
      continue;
    }
    // 如果子节点存在
    std::shared_ptr<TrieNode> tmp = cur->children_[c]->Clone();
    if (i - key.length() + 1 == 0) {
      tmp = std::make_shared<TrieNodeWithValue<T>>(tmp->children_, std::make_shared<T>(std::move(value)));
    }
    cur->children_[c] = tmp;
    cur = tmp;
  }
  return new_trie;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  //  throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  // if the key is empty
  if (key.empty()) {
    std::shared_ptr<TrieNode> new_root = root_->Clone();
    new_root = std::make_shared<TrieNode>(new_root->children_);
    return Trie(new_root);
  }

  std::shared_ptr<TrieNode> cur = this->root_->Clone();
  std::vector<std::shared_ptr<TrieNode>> path;
  int len = static_cast<int>(key.length());
  for (int i = 0; i < len; i++) {  // walk through the tree to find the end node for the key
    auto c = key[i];
    path.emplace_back(cur);
    if (!cur->children_[c]) {
      // the key does not exist
      return Trie(path.front());
    }
    std::shared_ptr<TrieNode> tmp = cur->children_[c]->Clone();
    cur->children_[c] = tmp;
    cur = tmp;
  }
  // cur is the end node, and cur not in path
  cur = std::make_shared<TrieNode>(cur->children_);
  path.back()->children_[key[len - 1]] = cur;

  for (int i = len - 1; i >= 0; i--) {
    auto c = key[i];
    auto parent = path.back();
    if (cur->children_.empty() && !cur->is_value_node_) {
      // 如果cur节点没有儿子并且不带有value，则cur的父亲要跟cur断绝关系
      parent->children_.erase(c);
    }
    cur = parent;
    path.pop_back();
  }
  if (cur->children_.empty() && !cur->is_value_node_) {
    return Trie(nullptr);
  }

  return Trie(cur);
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
