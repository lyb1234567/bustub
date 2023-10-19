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
/*
 初始化内部页面
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size)
{
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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key)
{
  array_[index].first=key;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value)
{
  array_[index].second=value;
}
/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType
{
  return array_[index].second;
}



/**
 * 在B+树的内部页面中合并当前页面和传入的右侧页面。
 *
 * @tparam KeyType 键的数据类型。
 * @tparam ValueType 值的数据类型，对于B+树内部页面，通常为`page_id_t`。
 * @tparam KeyComparator 键比较器的类型。
 *
 * @param key 需要插入的键，通常是从父页面中传下来的，标记两个页面的分隔点。
 * @param right_page 指向要合并的右侧页面的指针。
 * @param buffer_pool_manager_ 缓冲池管理器，用于管理页的加载、卸载和删除。
 *
 * 该函数执行以下步骤：
 * 1. 插入从父页面传递下来的键和右侧页面的第一个值。
 * 2. 从右侧页面复制所有键值对到当前页面。
 * 3. 在合并后，右侧页面将被标记为脏页，因为其内容已更改。
 * 4. 最后，右侧页面从缓冲池中删除。
 * 5. 所有从右侧页面移动到当前页面的子页面（如果有）的父页面ID都更新为当前页面的ID。
 *
 * 示例：
 * 假设当前页面键值对为[(10, A)]与对应的页面ID [Z, A]，传入的右侧页面键值对为[(30, D)]与对应的页面ID [C, D]，
 * 传入的键为20。
 * 调用此函数后，当前页面的键值对将变为[(10, A), (20, C), (30, D)]，
 * 页面ID可能变为[Z, A, C, D]。右侧页面将从缓冲池中删除。
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Merge(const KeyType &key, Page *right_page,
                                           BufferPoolManager *buffer_pool_manager_) -> void {
  auto right = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(right_page->GetData());
  int size = GetSize();
  array_[GetSize()] = std::make_pair(key, right->ValueAt(0));
  IncreaseSize(1);
  for (int i = GetSize()+1, j = 1; j < right->GetSize(); i++, j++) {
    array_[i] = std::make_pair(right->KeyAt(j), right->ValueAt(j));
    IncreaseSize(1);
  }
  buffer_pool_manager_->UnpinPage(right->GetPageId(), true);
  buffer_pool_manager_->DeletePage(right->GetPageId());
  for (int i = size; i < GetSize(); i++) {
    page_id_t child_page_id = ValueAt(i);
//    更新所有孩子节点的父页面的指针
    auto child_page = buffer_pool_manager_->FetchPage(child_page_id);
    auto child_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(child_page->GetData());
    child_node->SetParentPageId(GetPageId());
    buffer_pool_manager_->UnpinPage(child_page_id, true);
  }
}

/**
 * 在B+树的内部页面中分裂页面。
 *
 * @tparam KeyType 键的数据类型。
 * @tparam ValueType 值的数据类型，对于B+树内部页面，通常为`page_id_t`。
 * @tparam KeyComparator 键比较器的类型。
 *
 * @param key 用于分裂的键。
 * @param page_bother 与当前页面关联的新页面。
 * @param page_parent_page 父页面的引用。
 * @param keyComparator 用于比较键的比较器。
 * @param buffer_pool_manager_ 缓冲池管理器实例。
 *
 * 当B+树的一个内部页面因插入操作而达到其最大容量时，需要将其分裂为两个页面以维持B+树的性质。
 * 此函数执行这一操作，将当前页面的一部分键和子页面ID移动到一个新的内部页面（page_bother）中，
 * 并将用于分裂的键添加到其父页面中。
 *
 * 示例：
 * 假设一个内部页面的最大容量为5，当前键值对为[(10, A), (20, B), (30, C), (40, D), (50, E)]，
 * 当我们尝试插入键值对(35, F)时，页面将被分裂为两部分，例如：[(10, A), (20, B), (30, C)]和[(35, F), (40, D), (50, E)]，
 * 其中一个键（例如30或35）将被添加到其父页面中。
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Split(const KeyType &key, Page *page_bother, Page *page_parent_page,
                                           const KeyComparator &keyComparator,
                                           BufferPoolManager *buffer_pool_manager_) {
//  首先声明一个GetMaxSize()+1个内存
  auto *tmp = static_cast<MappingType *>(malloc(sizeof(MappingType) * (GetMaxSize() + 1)));
  // 将 key，page_bother->GetPageId() 插入，利用 tmp 来防止溢出
//  声明一个布尔变量来判断是否找到特定的分裂的位置
  bool flag = true;

  tmp[0] = array_[0];
  // 遍历当前内部页面的所有元素，如果当前页面的元素小于特定的key，插入前半段
  for (int i = 1; i < GetMaxSize(); i++) {
    if (keyComparator(array_[i].first, key) < 0) {
      tmp[i] = array_[i];
//     将特定的分裂的键插入，将后面的元素插入
    } else if (flag && keyComparator(array_[i].first, key) > 0) {
      flag = false;
      tmp[i] = std::make_pair(key, page_bother->GetPageId());
      tmp[i + 1] = array_[i];
    } else {
      tmp[i + 1] = array_[i];
    }
  }
//  如果没有找，直接在尾部插入
  if (flag) {
    tmp[GetMaxSize()] = std::make_pair(key, page_bother->GetPageId());
  }


  auto page_bother_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(page_bother->GetData());
  page_bother_node->SetParentPageId(GetPageId());
  IncreaseSize(1);
  // split，前半截不动，后半截移动到新的页上
  int mid = (GetMaxSize() + 1) / 2;
  auto page_parent_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(page_parent_page->GetData());
  for (int i = 0; i < mid; i++) {
    array_[i] = tmp[i];
  }
  int i = 0;
//  现在读写后半段，因为是将元素吸入新的的页面上，所有涉及到ping的操作
  while (mid <= (GetMaxSize())) {
    Page *child = buffer_pool_manager_->FetchPage(tmp[mid].second);
    auto child_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(child->GetData());
    child_node->SetParentPageId(page_parent_node->GetPageId());
    page_parent_node->array_[i++] = tmp[mid++];
    page_parent_node->IncreaseSize(1);
    IncreaseSize(-1);
    buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
  }
  free(tmp);
}
/**
 * 在B+树的内部页面中，为给定子页面ID查找其兄弟页面。
 *
 * @tparam KeyType 键的数据类型。
 * @tparam ValueType 值的数据类型，对于B+树内部页面，通常为`page_id_t`。
 *
 * @param child_page_id 需要查找其兄弟页面的子页面ID。
 * @param[out] bother_page 返回找到的兄弟页面。
 * @param[out] key 两个子页面（给定的页面和其兄弟页面）之间的键。
 * @param[out] ispre 返回的兄弟页面是在给定子页面的左侧还是右侧。如果为true，表示在左侧；否则表示在右侧。
 * @param buffer_pool_manager_ 缓冲池管理器实例。
 *
 * 当一个页面的键数量降至其最小值以下时，可能需要从其兄弟页面借一个键或与其兄弟页面合并。
 * 此函数用于定位合适的兄弟页面来执行这些操作。
 *
 * 示例：
 * 假设内部页面中的子页面ID为[A, B, C]，其键为[20, 50]。
 * 调用此函数以查找子页面B的兄弟页面可能会返回子页面A或C，取决于它们的键数量。
 * 如果选择了子页面A作为兄弟页面，`ispre`将为true，并且`key`将为20。
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetBotherPage(bustub::page_id_t child_page_id, bustub::Page *&bother_page, KeyType &key, bool &ispre, bustub::BufferPoolManager *buffer_pool_manager_)
{
  int i=0;
//  找到child_page_id位于当前页面的索引位置
  for(i=0;i<GetSize();i++)
  {
    if(ValueAt(i)==child_page_id)
    {
      break;
    }
  }

  if (i >= 1) {
    bother_page = buffer_pool_manager_->FetchPage(ValueAt(i - 1));
    bother_page->WLatch();
    key = KeyAt(i);
    ispre = true;
    return;
  }
  bother_page = buffer_pool_manager_->FetchPage(ValueAt(i + 1));
  bother_page->WLatch();
  key = KeyAt(i + 1);
  ispre = false;
}
/**
 * 使用二分查找在内部B+树页面中查找键。
 * 其目的是找到适当的子页面以继续搜索。
 *
 * 示例：
 * 考虑以下场景：我们有一个内部节点，其键为10, 20, 30，因此有4个子节点的引用（或页ID），假设为A, B, C, D。这意味着：
小于10的键应该进入子节点A
在10和20之间的键应该进入子节点B
在20和30之间的键应该进入子节点C
大于30的键应该进入子节点D
 * 如果在内部页面中有键[10, 20, 30]，我们想查找键25，
 * 该函数将返回与键20关联的值，这是小于或等于25的键的子引用。
 *
 * @param key           需要查找的键。
 * @param keyComparator 键的比较器。
 * @return              与输入键小于或等于的第一个键关联的值（页面ID）。
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &keyComparator) -> ValueType
{
  int l=1;
  int r=GetSize();
  while(l<r)
  {
     int mid=(l+r)/2;
     if(keyComparator(array_[mid].first,key)<=0)
     {
        l=mid+1;
     }
     else
     {
        r=mid;
     }
  }
  return array_[r-1].second;
}
/**
 * 使用二分查找确定指定键在B+树内部页面中的位置。
 *
 * @tparam KeyType 键的数据类型。
 * @tparam ValueType 值的数据类型，对于B+树内部页面，通常为`page_id_t`。
 * @tparam KeyComparator 键比较器的类型。
 * @param key 要查找的键。
 * @param keyComparator 用于比较键的比较器。
 * @return 返回键应该被插入的索引位置。
 *
 * 示例：
 * 如果在内部页面中有键[10, 20, 30]，并且我们想查找键25的索引，
 * 该函数将返回2，这是25应插入的位置。
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &keyComparator) -> int
{
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
/**
 * 在B+树内部页面中插入新的第一个键值对。
 *
 * @tparam KeyType 键的数据类型。
 * @tparam ValueType 值的数据类型，对于B+树内部页面，通常为`page_id_t`。
 * @tparam KeyComparator 键比较器的类型。
 *
 * @param key 要插入的键。
 * @param value 要插入的值，通常为页面ID。
 *
 * 该函数通过将所有现有的键值对向右移动来为新的键值对腾出空间。
 * 新的键值对会被插入到数组的最前面，并且页面的大小会增加1。
 *
 * 示例：
 * 假设内部页面中的键值对为[(20, B), (30, C)]与对应的页面ID [A, B, C]，
 * 调用此函数以插入键值对(10, Z)后，键值对将变为[(10, A), (20, B), (30, C)]，
 * 页面ID变为[Z, A, B, C]，并且页面的大小也会增加1。
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertFirst(const KeyType &key, const ValueType &value)
{
   for(int i=GetSize();i>0;i--)
   {
     array_[i]=array_[i-1];
   }
   SetKeyAt(1,key);
   SetValueAt(0,value);
   IncreaseSize(1);
}
/**
 * 在B+树的内部页面中插入一个键值对。
 *
 * @tparam KeyType 键的数据类型。
 * @tparam ValueType 值的数据类型，对于B+树内部页面，通常为`page_id_t`。
 * @tparam KeyComparator 键比较器的类型。
 *
 * @param value 包含键和值的键值对。
 * @param keyComparator 用于比较键的比较器。
 *
 * 该函数将尝试在正确的位置插入键值对，保持页面中的键排序。
 * 如果找到了正确的插入位置，函数将插入键值对并提前返回。
 * 如果没有找到正确的位置，键值对将被插入到页面的最前面。
 *
 * 示例：
 * 假设内部页面中的键值对为[(10, A),(20,B) (30, C)]与对应的页面ID [Z, A, B,C]，
 * 调用此函数以插入键值对(15, G)后，键值对将变为[(10, A),(15,G) (20, B), (30, C)]，
 * 页面ID可能变为[Z, A, G, B, C]，并且页面的大小也会增加1。
 *
 * 假如遍历完了依旧没有找到可以插入的位置,那么就代表需要插在最前面比如（6，G),那么需要主义的是，因为原先数组中第0位索引的key肯定是invalid，所以我们永远是插在第1位索引的位置的
 * 也就是[(6,G),(10, A),(20,B) (30, C)]然后页面ID就是 [Z,G,A,B,C]
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const std::pair<KeyType, ValueType> &value, const KeyComparator &keyComparator)
{
    for(int i=GetSize()-1;i>0;i--)
   {
     if (keyComparator(array_[i].first,value.first)>0)
     {
          array_[i + 1] = array_[i];
     }
     else
     {
          array_[i+1]=value;
          IncreaseSize(1);
          return;
     }

   }
   SetValueAt(1,value.second);
   SetKeyAt(1,value.first);
   IncreaseSize(1);
}
/**
 * 删除B+树内部页面中的第一个键值对。
 *
 * @tparam KeyType 键的数据类型。
 * @tparam ValueType 值的数据类型，对于B+树内部页面，通常为`page_id_t`。
 * @tparam KeyComparator 键比较器的类型。
 *
 * 该函数通过移动所有其他键值对向左来删除第一个键值对，并更新页面的大小。
 *
 * 示例：
 * 如果内部页面中有键值对[(10, B), (20, C), (30, D)]与对应的页面ID [A, B, C, D]，
 * 调用此函数后，键值对将变为[(20, C), (30, D)]，页面ID变为[B, C, D]，
 * 并且页面的大小也会减少1。
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteFirst()
{
   for (int i = 1; i < GetSize(); i++) {
     array_[i - 1] = array_[i];
   }
   IncreaseSize(-1);
}


/**
 * 在B+树的内部页面中删除给定的键（如果存在）。
 *
 * @tparam KeyType 键的数据类型。
 * @tparam ValueType 值的数据类型，对于B+树内部页面，通常为`page_id_t`。
 * @tparam KeyComparator 键比较器的类型。
 *
 * @param key 要删除的键。
 * @param keyComparator 用于比较键的比较器。
 *
 * @return 如果成功删除了键，则返回true，否则返回false。
 *
 * 函数首先使用二分查找来确定要删除的键在数组中的位置。
 * 如果找到了该键，则将其后面的所有键值对向左移动以覆盖它，并将页面大小减小1。
 * 如果键不存在，则直接返回false。
 *
 * 示例：
 * 假设内部页面中的键值对为[(10, A), (20, B), (30, C)]与对应的页面ID [Z, A, B, C]，
 * 调用此函数以删除键20后，键值对将变为[(10, A), (30, C)]，页面ID变为[Z, A, C]，
 * 并且页面的大小也会减少1。
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Delete(const KeyType &key, const KeyComparator &keyComparator) -> bool
{
   int index= KeyIndex(key,keyComparator);
   if (index>=GetSize() || keyComparator(KeyAt(index),key)!=0)
   {
      return false;
   }
   for(int i=index+1;i<GetSize();i++)
   {
      array_[i-1]=array_[i];
   }
   IncreaseSize(-1);
   return true;
}
// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub