#include "primer/trie.h"
#include <memory>
#include <string_view>
#include <utility>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  if (this->root_ == nullptr) {
    return nullptr;
  }

  std::shared_ptr<const TrieNode> parent = this->root_;

  for (char ch : key) {
    auto p = parent->children_.find(ch);
    // 未找到要匹配的下一个字符
    if (p == parent->children_.end()) {
      return nullptr;
    }

    parent = p->second;
  }

  const auto *res = dynamic_cast<const TrieNodeWithValue<T> *>(parent.get());

  if (res != nullptr) {
    return res->value_.get();
  }
  return nullptr;
}

template <class T>
std::shared_ptr<TrieNode> RecursePutValue(std::shared_ptr<TrieNode> &new_root, std::string_view key, T value) {
  // key为空的情况单独处理
  if (key.empty()) {
    std::shared_ptr<T> val_p = std::make_shared<T>(std::move(value));
    new_root = std::make_shared<TrieNodeWithValue<T>>(new_root->children_, val_p);
    return new_root;
  }

  auto pos_child = new_root->children_.find(key[0]);
  // 找到
  if (pos_child != new_root->children_.end()) {
    if (key.size() > 1) {
      std::shared_ptr<TrieNode> next_root = pos_child->second->Clone();
      pos_child->second = RecursePutValue<T>(next_root, key.substr(1), std::move(value));
    } else if (key.size() == 1) {
      std::shared_ptr<T> val_p = std::make_shared<T>(std::move(value));
      pos_child->second = std::make_shared<TrieNodeWithValue<T>>(pos_child->second->children_, val_p);
    }
  } else {  // 未找到
    std::shared_ptr<const TrieNode> son_node(new TrieNode());
    std::pair<char, std::shared_ptr<const TrieNode>> new_pair(key[0], son_node);
    new_root->children_.insert(new_pair);

    new_root = RecursePutValue<T>(new_root, key, std::move(value));
  }

  return new_root;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  // 将常量改成非常量，否则后面会比较麻烦;检查root_是否为空
  std::shared_ptr<TrieNode> new_root = nullptr;
  if (this->root_ == nullptr) {
    new_root = std::make_shared<TrieNode>();
  } else {
    new_root = this->root_->Clone();
  }

  auto res_root = RecursePutValue<T>(new_root, key, std::move(value));

  return Trie(std::move(res_root));
}

std::shared_ptr<TrieNode> RecurseRemove(std::shared_ptr<TrieNode> &new_root, std::string_view key) {
  // 将key=0和key>0的情况分开处理的原因：
  // key=0时，节点要么为空，要么变成一个新的不带值节点
  // key>0时，需要将上一级的map去除一个（包括char和节点）
  if (key.empty()) {
    // 有值
    if (new_root->is_value_node_) {
      // 有孩子
      if (!new_root->children_.empty()) {
        std::shared_ptr<TrieNode> root_without_value = std::make_shared<TrieNode>(new_root->children_);
        return root_without_value;
      }
      // 无孩子
      return nullptr;
    }
    // 无值
    return new_root;
  }

  auto pos_child = new_root->children_.find(key[0]);
  // 找到,没找到没有说明，不做处理
  if (pos_child != new_root->children_.end()) {
    if (key.size() > 1) {
      std::shared_ptr<TrieNode> next_root = pos_child->second->Clone();
      std::shared_ptr<TrieNode> new_node = RecurseRemove(next_root, key.substr(1));

      // 检查返回的新的子节点（可能被处理成了空）
      if (new_node == nullptr) {
        new_root->children_.erase(pos_child);
      } else {
        pos_child->second = new_node;
      }
    } else {
      // 目标节点无孩子
      if (pos_child->second->children_.empty()) {
        new_root->children_.erase(pos_child);
      } else {  // 目标节点有孩子
        std::shared_ptr<TrieNode> node_without_value = std::make_shared<TrieNode>(pos_child->second->children_);
        pos_child->second = node_without_value;
      }
    }
  }

  // 既无子节点也无值，直接返回空
  if (new_root->children_.empty() && !new_root->is_value_node_) {
    return nullptr;
  }

  return new_root;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  std::shared_ptr<TrieNode> new_root = nullptr;
  if (this->root_ == nullptr) {
    new_root = std::make_shared<TrieNode>();
  } else {
    new_root = this->root_->Clone();
  }

  auto res_root = RecurseRemove(new_root, key);

  return Trie(std::move(res_root));
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
