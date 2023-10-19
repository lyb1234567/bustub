//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size)
{
        SetNextPageId(INVALID_PAGE_ID);
        SetParentPageId(parent_id);
        SetPageId(page_id);
        SetMaxSize(max_size);
        SetPageType(IndexPageType::LEAF_PAGE);
        SetSize(0);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_;}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {next_page_id_=next_page_id;}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType
{
  return array_[index].second;
}

/**
 * 在B+树的叶子页面中查找键的索引位置。
 *
 * @tparam KeyType 键的数据类型。
 * @tparam ValueType 值的数据类型，对于B+树的叶子页面，这通常是与键关联的记录的标识符或指针。
 * @tparam KeyComparator 键比较器的类型。
 *
 * @param key 要查找的键。
 * @param keyComparator 用于比较键的比较器。
 * @return 返回键在叶子页面中的索引位置。如果键不在页面中，则返回该键应该插入的位置。
 *
 * 此函数使用二分查找算法在排序的叶子页面中快速查找键的索引位置。
 * 如果键在页面中，则返回键的索引；否则，返回键应该插入的位置，以保持键的排序顺序。
 *
 * 示例：
 * 假设叶子页面的键值对为[(10, A), (20, B), (30, C)]，
 * 调用此函数以查找键15的索引位置，将返回索引1，表示键15应该插入索引1的位置。
 */

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &keyComparator) -> int
{
  int l=0;
  int r=GetSize();
  while(l<r)
  {
    int mid=(l+r)/2;
    if (keyComparator(array_[mid].first,key)<0)
    {
      l=mid+1;
    }
    else
    {
      r=mid;
    }
  }
  return l;
}

/**
 * 在B+树的叶子页面的首部插入一个键值对。
 *
 * @tparam KeyType 键的数据类型。
 * @tparam ValueType 值的数据类型，对于B+树的叶子页面，这通常是与键关联的记录的标识符或指针。
 * @tparam KeyComparator 键比较器的类型。
 *
 * @param key 要插入的键。
 * @param value 要插入的值。
 *
 * 此函数将键值对插入到叶子页面的首部。为了保持键的顺序，它会将现有的所有键值对向右移动一个位置，然后在首部插入新的键值对。
 *
 * 示例：
 * 假设叶子页面的键值对为[(10, A), (20, B), (30, C)]，
 * 调用此函数以插入键值对(5, Z)后，键值对将变为[(5, Z), (10, A), (20, B), (30, C)]。
 */

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertFirst(const KeyType &key, const ValueType &value)
{
  for(int i=GetSize();i>0;i--)
  {
    array_[i]=array_[i-1];
  }
  array_[0]=std::make_pair(key,value);
  IncreaseSize(1);
}

/**
 * 在B+树的叶子页面的末尾插入一个键值对。
 *
 * @tparam KeyType 键的数据类型。
 * @tparam ValueType 值的数据类型，对于B+树的叶子页面，这通常是与键关联的记录的标识符或指针。
 * @tparam KeyComparator 键比较器的类型。
 *
 * @param key 要插入的键。
 * @param value 要插入的值。
 *
 * 此函数将键值对直接插入到叶子页面的末尾，无需对现有的键值对进行任何移动。
 * 该操作相对较快，因为它只需一次数组赋值和增加页面大小。
 *
 * 示例：
 * 假设叶子页面的键值对为[(10, A), (20, B), (30, C)]，
 * 调用此函数以插入键值对(40, D)后，键值对将变为[(10, A), (20, B), (30, C), (40, D)]。
 */

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertLast(const KeyType &key, const ValueType &value)
{
  array_[GetSize()]=std::make_pair(key,value);
  IncreaseSize(1);
}
/**
 * 在B+树的叶子页面中插入一个键值对。
 *
 * @tparam KeyType 键的数据类型。
 * @tparam ValueType 值的数据类型，对于B+树内部页面，通常为`page_id_t`。
 * @tparam KeyComparator 键比较器的类型。
 *
 * @param value 要插入的键和值的键值对。
 * @param index 预计插入键值对的索引位置。
 * @param keyComparator 用于比较键的比较器。
 *
 * @return 如果插入成功返回true，如果键已存在返回false。
 *
 * 该函数在指定的索引位置尝试插入键值对，如果指定位置的键与要插入的键相同，则插入失败，这确保了插入的叶子KV对的唯一性。
 * 否则，函数将根据提供的索引插入键值对，并将所有在该索引之后的键值对向右移动一个位置。
 *
 * 示例：
 * 假设叶子页面的键值对为[(10, A),(20,B),(30, C)]，
 * 调用此函数以索引1插入键值对(15, G)后，键值对将变为[(10, A),(15,G),(20, B),(30, C)]。
 *
 * 但是，如果调用此函数在索引1插入键值对(20, G)，插入会失败并返回false，因为键20已经存在。
 */

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(std::pair<KeyType, ValueType> value, int index, const KeyComparator &keyComparator) -> bool
{
  //如果给定的索引指向当前节点的一个有效位置，并且该位置上的键与要插入的键相同（通过keyComparator检查），则插入失败并返回false。这确保了B+树中的键是唯一的。
  if (index<GetSize() && keyComparator(value.first,array_[index].first)==0)
  {
    return false;
  }
  for (int i = GetSize() - 1; i >= index; i--) {
    array_[i + 1] = array_[i];
  }
  array_[index]=value;
  IncreaseSize(1);
  return true;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetPair(int index) -> std::pair<KeyType, ValueType> &
{
  return  array_[index];
}
/**
 * 在B+树的叶子页面中移除一个键值对。
 *
 * @tparam KeyType 键的数据类型。
 * @tparam ValueType 值的数据类型，对于B+树的叶子页面，这通常是与键关联的记录的标识符或指针。
 * @tparam KeyComparator 键比较器的类型。
 *
 * @param key 要移除的键。
 * @param index 要移除的键在页面中的索引位置。
 * @param keyComparator 用于比较键的比较器。
 * @return 如果键成功被移除，则返回true；否则返回false。
 *
 * 此函数首先检查给定索引位置的键是否与要移除的键相匹配。
 * 如果它们不匹配，则函数返回false，表示键不在叶子页面中。
 * 如果它们匹配，则该键值对从页面中移除，并且页面中后面的键值对向前移动一个位置来填补空白。
 * 最后，页面的大小减少1。
 *
 * 示例：
 * 假设叶子页面的键值对为[(10, A), (20, B), (30, C)]，
 * 调用此函数以移除键20，并提供索引1，将导致叶子页面的键值对变为[(10, A), (30, C)]。
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, int index, const KeyComparator &keyComparator) -> bool
{
//  检查是否是正确的index
  if (keyComparator(array_[index].first,key)!=0)
  {
    return false;
  }

//  移动元素
  for(int i=index;i<GetSize()-1;i++)
  {
    array_[i]=array_[i+1];
  }
  IncreaseSize(-1);
  return true;
}
/**
 * 在B+树的叶子页面中删除一个键。
 *
 * @tparam KeyType 键的数据类型。
 * @tparam ValueType 值的数据类型，对于B+树的叶子页面，这通常与RID相关。
 * @tparam KeyComparator 键比较器的类型。
 *
 * @param key 要删除的键。
 * @param keyComparator 用于比较键的比较器。
 *
 * 该函数首先使用`KeyIndex`方法确定要删除的键在页面中的位置。然后，如果找到匹配的键，它会移动后续的键来覆盖要删除的键。
 * 最后，页面的大小减少1，以反映键的删除。
 *
 * 示例：
 * 假设叶子页面中的键值对为[(10, A),(15,G) (20, B), (30, C)]，调用此函数以删除键15后，
 * 键值对将变为[(10, A),(20, B), (30, C)]，并且页面的大小也会减少1。
 *
 * @return 如果成功删除键，则返回true；否则，如果在页面中没有找到键，则返回false。
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Delete(const KeyType &key, const KeyComparator &keyComparator) -> bool
{
  int index= KeyIndex(key,keyComparator);
  if (index > GetSize() || keyComparator(KeyAt(index),key)!=0)
  {
    return false;
  }

  for (int i = index + 1; i < GetSize(); i++) {
    array_[i - 1] = array_[i];
  }
  IncreaseSize(-1);
  return true;
}
/**
 * 在B+树的叶子节点中进行合并操作。
 *
 * @tparam KeyType 键的数据类型。
 * @tparam ValueType 值的数据类型，对于B+树叶子页面，可能是实际的数据值或对应的数据引用。
 *
 * @param right_page 从其中合并数据的右侧叶子页面。
 * @param buffer_pool_manager_ 用于管理页面的缓冲池管理器。
 *
 * 该函数将从指定的右侧叶子页面中取出所有键值对，并将其追加到当前叶子页面的末尾。
 * 合并完成后，右侧的叶子页面将被设置为空，并在缓冲池管理器中被标记为脏页，然后被删除。
 *
 * 注意：该函数假设当前叶子页面有足够的空间来容纳来自右侧页面的所有数据。合并操作之前应确保这一点。
 *
 * 示例：
 * 假设当前叶子页面的键值对为[(10, A),(20,B)]，右侧叶子页面的键值对为[(30, C),(40, D)]。
 * 调用此函数后，当前叶子页面的键值对将变为[(10, A),(20,B),(30, C),(40, D)]，而右侧页面将被清空并删除。
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Merge(bustub::Page *right_page, bustub::BufferPoolManager *buffer_pool_manager_)
{
  auto right=reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(right_page->GetData());
//  从右边开始迁移页面元素
  for (int i = GetSize(), j = 0; j < right->GetSize(); j++, i++) {
    array_[i] = std::make_pair(right->KeyAt(j), right->ValueAt(j));
    IncreaseSize(1);
  }
//  迁移完之后执行ping操作
  right->SetSize(0);
  buffer_pool_manager_->UnpinPage(right_page->GetPageId(), true);
  buffer_pool_manager_->DeletePage(right_page->GetPageId());
}
/**
 * 在B+树的叶子页面中分裂一个页面。
 *
 * @tparam INDEX_TEMPLATE_ARGUMENTS 参数列表，包括键的类型、值的类型和键的比较器类型。
 *
 * @param bother_page 指向需要分裂成的新兄弟页面的指针。
 *
 * 该函数将当前页面（`this`页面）中的键值对分为两半：前一半保留在当前页面中，后一半迁移到新的“兄弟”页面中。
 * 这样做的目的是保持B+树的平衡，当当前页面因插入操作而溢出时需要分裂页面。
 *
 * 此外，该函数还更新叶子页面间的链接，确保双向链表结构在分裂后保持正确。
 *
 * 示例：
 * 假设当前页面的键值对为[(5, A), (10, B), (15, C), (20, D), (25, E), (30, F)]。
 * 调用此函数后，当前页面的键值对可能变为[(5, A), (10, B), (15, C)]，
 * 而新的“兄弟”页面的键值对则为[(20, D), (25, E), (30, F)]。
 * 同时，新的“兄弟”页面的`next_page_id_`将指向当前页面原来的`next_page_id_`，而当前页面的`next_page_id_`则更新为新的“兄弟”页面的ID。
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Split(bustub::Page *bother_page)
{
  int mid=GetSize()/2;
//  生成一个新的兄弟兄弟页面用于储存分割点之后的所有KV对
  auto bother_leaf_page=reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(bother_page->GetData());
  for(int j=0,i=mid;i<GetMaxSize();i++,j++)
  {
    bother_leaf_page->array_[j]=array_[i];
    IncreaseSize(-1);
    bother_leaf_page->IncreaseSize(1);
  }
//  原来的页面指向的next page现在由bother page指向，比如说原来是A->C，但是现在A分裂成A和B，A的后半部分元素移到B上面，所以现在变成A->B->C
  bother_leaf_page->next_page_id_=next_page_id_;
  SetNextPageId(bother_page->GetPageId());
}
template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub