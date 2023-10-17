//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }


/**
 * @brief 在内部页中查找给定键的值。sd
 * 
 * 该函数通过键比较器使用二分查找法在内部页中找到给定键或第一个大于给定键的条目。
 * 然后，它返回该条目的值，该值通常是子树（或记录）的页面ID，在该子树（或记录）中可以找到具有给定键的条目。
 * 如果所有条目的键都小于给定键，则返回最后一个条目的值。
 * 
 * @param key 需要在内部页中进行查找的键。
 * @param keyComparator 用于比较键值的比较器函数。
 * @return ValueType 返回与找到的键关联的值，如果键未找到，则返回第一个大于键的条目的值，或者如果所有条目都小于给定键，则返回最后一个条目的值。
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &keyComparator) -> ValueType {
  int l = 1;
  int r = GetSize();
  while (l < r) {
    int mid = (l + r) / 2;
    if (keyComparator(array_[mid].first, key) <= 0) {
      l = mid + 1;
    } else {
      r = mid;
    }
  }
  return array_[r - 1].second;
}


/**
 * @brief 在B+树的内部页中插入一个键值对。
 * 
 * @tparam KeyType 键的类型。
 * @tparam ValueType 值的类型，对于内部页面来说，值通常是子页面的ID。
 * @tparam KeyComparator 键的比较器，用于键的排序和比较。
 * @param value 要插入的键值对。
 * @param keyComparator 键的比较器对象。
 * 
 * @details
 * 该函数确保在插入之后键值对保持有序。如果找到合适的位置，则在该位置上插入新的键值对，
 * 并将后面的所有键值对向后移动一个位置。如果新的键值对应该被插入到第一个位置，那么特殊处理。
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const MappingType &value, const KeyComparator &keyComparator) -> void {
  for (int i = GetSize() - 1; i > 0; i--) {
    if (keyComparator(array_[i].first, value.first) > 0) {
      array_[i + 1] = array_[i];
    } else {
      array_[i + 1] = value;
      IncreaseSize(1);
      return;
    }
  }
  SetKeyAt(1, value.first);
  SetValueAt(1, value.second);
  IncreaseSize(1);
}



/**
 * @brief 对B+树的内部页面进行拆分。
 * 
 * @tparam KeyType 键的类型。
 * @tparam ValueType 值的类型。对于内部页面，值通常是子页面的ID。
 * @tparam KeyComparator 键的比较器，用于排序和比较。
 * @param key 要插入的键。
 * @param page_bother 拆分后的新兄弟页面。
 * @param page_parent_page 拆分后的新父页面。
 * @param keyComparator 键的比较器对象。
 * @param buffer_pool_manager_ 缓冲池管理器。
 * 
 * @details 
 * 当B+树的内部页面过大时，需要进行拆分。此函数:
 * 1. 创建一个临时数组，用于存放当前页面的键值对和新的待插入键，确保数组有序。
 * 2. 使用临时数组将当前页面的后半部分移动到新的兄弟页面。
 * 3. 为所有子页面更新父页面ID。
 * 4. 释放临时数组。
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Split(const KeyType &key, Page *page_bother, Page *page_parent_page,
                                           const KeyComparator &keyComparator,
                                           BufferPoolManager *buffer_pool_manager_) {
  
  // 创建一个临时数组来存放所有的键值对。
  auto *tmp = static_cast<MappingType *>(malloc(sizeof(MappingType) * (GetMaxSize() + 1)));

  // 将键和page_bother->GetPageId()插入到临时数组中，确保数组有序。
  bool flag = true;
  tmp[0] = array_[0];
  for (int i = 1; i < GetMaxSize(); i++) {
    if (keyComparator(array_[i].first, key) < 0) {
      tmp[i] = array_[i];
    } else if (flag && keyComparator(array_[i].first, key) > 0) {
      flag = false;
      tmp[i] = std::make_pair(key, page_bother->GetPageId());
      tmp[i + 1] = array_[i];
    } else {
      tmp[i + 1] = array_[i];
    }
  }
  if (flag) {
    tmp[GetMaxSize()] = std::make_pair(key, page_bother->GetPageId());
  }
  
  // 计算用于拆分的中点。
  int mid = (GetMaxSize() + 1) / 2;
  auto page_parent_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(page_parent_page->GetData());

  // 将键值对的前半部分移动到当前页面。后半部分移动到新的兄弟页面。
  for (int i = 0; i < mid; i++) {
    array_[i] = tmp[i];
  }

 
//  后半截开始深度将
  int i = 0;
  while (mid <= GetMaxSize()) {
    Page *child = buffer_pool_manager_->FetchPage(tmp[mid].second);
    auto child_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(child->GetData());
    child_node->SetParentPageId(page_parent_node->GetPageId());

    page_parent_node->array_[i++] = tmp[mid++];
    page_parent_node->IncreaseSize(1);
    IncreaseSize(-1);
    buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
  }

  // 释放临时数组。
  free(tmp);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Delete(const KeyType &key, const KeyComparator &keyComparator) -> bool {
  int index = KeyIndex(key, keyComparator);
  if (index >= GetSize() || keyComparator(KeyAt(index), key) != 0) {
    return false;
  }
  for (int i = index + 1; i < GetSize(); i++) {
    array_[i - 1] = array_[i];
  }
  IncreaseSize(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &keyComparator) -> int {
  int l = 1;
  int r = GetSize();
  while (l < r) {
    int mid = (l + r) / 2;
    if (keyComparator(array_[mid].first, key) < 0) {
      l = mid + 1;
    } else {
      r = mid;
    }
  }
  return r;
}


INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetBotherPage(page_id_t child_page_id, Page *&bother_page, KeyType &key,
                                                   bool &ispre, BufferPoolManager *buffer_pool_manager_) -> void {
  int i = 0;
  for (i = 0; i < GetSize(); i++) {
    if (ValueAt(i) == child_page_id) {
      break;
    }
  }
  // 找到了对应的key，那么对应的在internal页面中有m个key和m+1个value，所以如果是从0开始遍历的话，如果找到了特定的child_id之后，那么左边一定也有一个兄弟节点，并记下对应的位置
  if (i >= 1) {
    bother_page = buffer_pool_manager_->FetchPage(ValueAt(i - 1));
    bother_page->WLatch();
    key = KeyAt(i);
    ispre = true;
    return;
  }
  // 如果遍历完了之后，一九没有找到，那么兄弟页面就是在末尾，几下位置。
  bother_page = buffer_pool_manager_->FetchPage(ValueAt(i + 1));
  bother_page->WLatch();
  key = KeyAt(i + 1);
  ispre = false;
}


// 指定B+树内部页面的模板参数
INDEX_TEMPLATE_ARGUMENTS
// 定义B+树内部页面的合并函数
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Merge(const KeyType &key, Page *right_page,
                                           BufferPoolManager *buffer_pool_manager_) -> void {
  // 从右页面获取数据
  auto right = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(right_page->GetData());

  // 记录当前页面的大小
  int size = GetSize();

  // 将键和右页面的第一个值添加到当前页面
  array_[GetSize()] = std::make_pair(key, right->ValueAt(0));
  IncreaseSize(1);

  // 将右页面的其他键和值也复制到当前页面
  for (int i = GetSize(), j = 1; j < right->GetSize(); i++, j++) {
    array_[i] = std::make_pair(right->KeyAt(j), right->ValueAt(j));
    IncreaseSize(1);
  }

  // 从缓冲池中解除固定并删除右页面
  buffer_pool_manager_->UnpinPage(right->GetPageId(), true);
  buffer_pool_manager_->DeletePage(right->GetPageId());

  // 为所有新的子页面设置父页面ID
  for (int i = size; i < GetSize(); i++) {
    page_id_t child_page_id = ValueAt(i);
    auto child_page = buffer_pool_manager_->FetchPage(child_page_id);
    auto child_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(child_page->GetData());
    child_node->SetParentPageId(GetPageId());
    buffer_pool_manager_->UnpinPage(child_page_id, true);
  }
}

// 在当前页面头部插入一个KV对，因为第一个是invalid的，所以我们将value插在0而key插在1
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertFirst(const KeyType &key, const ValueType &value) -> void {
  for (int i = GetSize(); i > 0; i--) {
    array_[i] = array_[i - 1];
  }
  SetValueAt(0, value);
  SetKeyAt(1, key);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteFirst() -> void {
  for (int i = 1; i < GetSize(); i++) {
    array_[i - 1] = array_[i];
  }
  IncreaseSize(-1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub