#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_==INVALID_PAGE_ID;}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
/**
 * 在B+树中搜索与给定键匹配的值。
 *
 * @tparam INDEX_TEMPLATE_ARGUMENTS 参数列表，包括键的类型、值的类型和键的比较器类型。
 *
 * @param key 要搜索的键。
 * @param result 存储搜索到的值的向量指针。
 * @param transaction 与此操作关联的事务。
 * @return 如果在B+树中找到与给定键匹配的值，则返回true，否则返回false。
 *
 * 该函数首先检查B+树是否为空。如果是空的，则直接返回false。
 * 如果树不为空，它会使用FindLeafPage函数来查找包含给定键的叶子页面。
 * 找到叶子页面后，函数会在页面中搜索键，并尝试获取与键匹配的值。
 * 如果找到值，则将其添加到结果向量中并返回true。
 * 如果在叶子页面中未找到键，则返回false。
 *
 * 无论操作是否成功，函数都将对叶子页面进行Unpin操作，以确保页面可以被其他操作使用。
 *
 * 示例：
 * 假设B+树有如下结构：
 *      10
 *     /  \
 *    5   15
 *   / \  / \
 *  1  7 12  20
 *
 * 其中，键7与值'Z'关联。
 * 如果搜索键7，函数将返回true并将'Z'添加到结果向量中。
 * 如果搜索键8，函数将返回false，因为键8在B+树中不存在。
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  if (IsEmpty())
  {
    return false;
  }
  auto page= FindLeafPage(key);
  if (!page)
  {
    return false;
  }

  auto leaf_page=reinterpret_cast<LeafPage *>(page->GetData());
  int index=leaf_page->KeyIndex(key,comparator_);
//  如果找到了合法的index，直接读取对应的值，并且Unping
  if (index<leaf_page->GetSize() && comparator_(leaf_page->KeyAt(index),key)==0)
  {
    result->push_back(leaf_page->ValueAt(index));
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return true;
  }
//  如果未能找到，也Unping对应的page_id
   buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
   return false;
}

/**
 * 在B+树中找到与给定键匹配的叶子页面。
 *
 * @tparam INDEX_TEMPLATE_ARGUMENTS 参数列表，包括键的类型、值的类型和键的比较器类型。
 *
 * @param key 要搜索的键。
 * @return 返回与给定键匹配的叶子页面的指针。
 *
 * 该函数从B+树的根页面开始搜索，然后沿着树向下遍历，直到找到与给定键匹配的叶子页面为止。
 * 在搜索过程中，函数使用键的比较器来确定每个内部节点的子页面选择。
 *
 * 请注意，如果B+树为空，此函数将返回nullptr。
 *
 * 示例：
 * 假设B+树有如下结构：
 *      10
 *     /  \
 *    5   15
 *   / \  / \
 *  1  7 12  20
 *
 * 对于键6，函数将从根页面开始，选择键小于10的子页面，然后在下一级选择键大于5的子页面，最终找到与键6匹配的叶子页面（其键范围为[5,7]）。
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key) -> Page *
{
  if (IsEmpty())
  {
    return nullptr;
  }
//  拉取根页面
  Page * curr_page=buffer_pool_manager_->FetchPage(root_page_id_);
  auto curr_page_iter=reinterpret_cast<InternalPage *>(curr_page->GetData());
//  遍历整个树
  while (!curr_page_iter->IsLeafPage())
  {
//   在当前的内部页面利用LookUp函数中的二分查找寻找特定的key
    Page *next_page=buffer_pool_manager_->FetchPage(curr_page_iter->Lookup(key,comparator_));
    auto next_page_iter=reinterpret_cast<InternalPage *> (next_page->GetData());
//    每次读取玩当前页面之后，就对当前页面进行unpin,并且因为我们并未修改页面中的内容，我们就不需要将当前页面标记为dirty
    buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);
    curr_page=next_page;
    curr_page_iter=next_page_iter;
  }
  return curr_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetMaxsize(BPlusTreePage *page) const -> int {
  return page->IsLeafPage() ? leaf_max_size_ - 1 : internal_max_size_;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * 在B+树中插入一个键值对。
 *
 * @tparam INDEX_TEMPLATE_ARGUMENTS 参数列表，包括键的类型、值的类型和键的比较器类型。
 *
 * @param key 要插入的键。
 * @param value 与键关联的值。
 * @param transaction 与此操作关联的事务。
 * @return 如果键值对成功插入到B+树中，则返回true，否则返回false。
 *
 * 该函数首先使用FindLeafPage函数查找应插入键值对的叶子页面。
 * 如果B+树为空，将创建一个新的叶子页面，并更新树的根页面ID。
 *
 * 如果找到适当的叶子页面，函数会尝试在其中插入键值对。
 * 如果键已经存在，则插入操作失败并返回false。
 *
 * 如果由于插入而使叶子页面达到其最大大小，则会进行分裂操作，
 * 并在父页面中插入一个新的键，该键是新页面中的最小键。
 *
 * 无论插入操作是否成功，函数都将释放（Unpin）涉及的所有页面，
 * 以确保它们可以被其他操作或缓冲池管理器使用。
 *
 * 示例：
 * 假设B+树有如下结构：
 *      10
 *     /  \
 *    5   15
 *   / \  / \
 *  1  7 12  20
 *
 * 调用Insert(6, 'Z')，键6和值'Z'将被插入，并且B+树可能需要进行相应的分裂和结构调整。
 * 如果再次调用Insert(6, 'Y')，由于键6已存在，插入操作将返回false。
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  Page * page_leaf= FindLeafPage(key);
  while(page_leaf== nullptr)
  {
//    如果是空的，首先就生成一个新的page
    if (IsEmpty())
    {
      page_id_t page_id;
      Page *page = buffer_pool_manager_->NewPage(&page_id);
      auto leaf_node=reinterpret_cast<LeafPage *>(page->GetData());
//      因为是空的，所以是没有父页面的指针的
      leaf_node->Init(page_id, INVALID_PAGE_ID, leaf_max_size_);
//      更新root page
      root_page_id_=page_id;
      UpdateRootPageId(root_page_id_);
      buffer_pool_manager_->UnpinPage(page_id, true);
    }
//    当树不为空时，就代表，有地方可以插了
    page_leaf= FindLeafPage(key);
  }
  auto leaf_node = reinterpret_cast<LeafPage *>(page_leaf->GetData());
//  找到key在叶子页面特定的位置
  int index=leaf_node->KeyIndex(key,comparator_);
//先尝试插入这个KV对，叶子页面的insert函树，会检查是否是插入的独一的KV对，如果K已经在叶子里面了，那么就直接失败
  bool retcode = leaf_node->Insert(std::make_pair(key, value), index, comparator_);
  if (!retcode)
  {
    buffer_pool_manager_->UnpinPage(page_leaf->GetPageId(), false);
    return false;
  }
//  如果插入成功之后，当前叶子页面的大小等于最大叶子页面大小，那么就需要分裂
  if(leaf_node->GetSize()==leaf_max_size_)
  {
//    生成一个兄弟页面，来储存分类出来的部分叶子页面的KV对
    page_id_t page_bother_id;
    Page *page_bother = buffer_pool_manager_->NewPage(&page_bother_id);
    auto leaf_bother_node = reinterpret_cast<LeafPage *>(page_bother->GetData());
    leaf_bother_node->Init(page_bother_id,INVALID_PAGE_ID,leaf_max_size_);
    leaf_node->Split(page_bother);
    // 父页需要插入一项，key = leaf_bother_node->KeyAt(0)，value = page_bother->GetPageId()
    InsertInParent(page_leaf, leaf_bother_node->KeyAt(0), page_bother);
    buffer_pool_manager_->UnpinPage(page_bother->GetPageId(), true);
  }
//  如果不需要分裂，那么就直接插入然后unping就好了
  buffer_pool_manager_->UnpinPage(page_leaf->GetPageId(), true);
  return true;
}
/**
 * 将分裂的子页面键值对插入到其父页面中。
 *
 * @tparam INDEX_TEMPLATE_ARGUMENTS 参数列表，包括键的类型、值的类型和键的比较器类型。
 *
 * @param page_leaf 分裂前的原始页面。
 * @param key 分裂后需要上移至父页面的键。
 * @param page_bother 由于分裂而新创建的页面。
 *
 * 当一个页面达到其最大容量并分裂时，该函数被调用以将一个键值对插入到其父页面中。
 * 该函数考虑了以下几种情况：
 *
 * 1. 当前页面是根页面：创建一个新的根页面，并将原始页面和分裂后的页面作为其子页面。
 * 2. 父页面有足够的空间：直接在父页面中插入键值对。
 * 3. 父页面没有足够的空间：需要将父页面分裂并可能递归地更新其父页面。
 *
 * 函数确保所有涉及的页面在操作结束时都会被正确地释放。
 *
 * 示例：
 * 假设B+树有如下结构，且每一级最多只能容纳两个元素：
 *      10
 *     /  \
 *    5   15
 *   / \  / \
 *  1  7 12  20
 *
 * 插入键21和22后，叶子页面和第二层内部页面都需要进行分裂。该函数确保键值对被正确地插入，并可能导致树的高度增加。
 */

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(bustub::Page *page_leaf, const KeyType &key, bustub::Page *page_bother)
{

auto tree_page=reinterpret_cast<BPlusTreePage*> (page_leaf->GetData());
//如果分裂前本身就身处于跟页面
if(tree_page->GetPageId()==root_page_id_)
{
   page_id_t  new_page_id;
   Page *new_page=buffer_pool_manager_->NewPage(&new_page_id);
   auto new_root=reinterpret_cast<InternalPage *>(new_page->GetData());
   new_root->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
   new_root->SetValueAt(0, page_leaf->GetPageId());
//   为新的根页面设置键和值,新创建的根页面包含两个值（子页面的页面ID）和一个键。其中，page_leaf是分裂之前的原始根页面，而page_bother是由于分裂而新创建的页面。
   new_root->SetValueAt(0, page_leaf->GetPageId());
   new_root->SetKeyAt(1, key);
   new_root->SetValueAt(1, page_bother->GetPageId());
   new_root->IncreaseSize(2);
//   原始根页面现在有了新的根，那么原来的就变成了子页面，需要更新父页面
   auto page_leaf_node=reinterpret_cast<BPlusTreePage*> (page_leaf->GetData());
   page_leaf_node->SetParentPageId(new_page_id);
//   新分裂出来的兄弟页面也是一样需要更新父页面
   auto page_bother_node = reinterpret_cast<BPlusTreePage *>(page_bother->GetData());
   page_bother_node->SetParentPageId(new_page_id);
//   更新根页面
   root_page_id_=new_page_id;
   UpdateRootPageId(root_page_id_);
   buffer_pool_manager_->UnpinPage(new_page_id, true);
   return;
}
//如果不是根页面，我们首先获取当前这个叶子页面的父页面
page_id_t parent_page_id=tree_page->GetParentPageId();
Page * parent_page=buffer_pool_manager_->FetchPage(parent_page_id);
auto parent_page_node=reinterpret_cast<InternalPage *>(parent_page->GetData());
auto page_bother_node=reinterpret_cast<InternalPage *>(page_bother->GetData());
//如果父页面有足够的空间，那我们直接插入就可以
if(parent_page_node->GetSize()<parent_page_node->GetMaxSize())
{
//   父页面插入分裂的中间节点的key，以及bother的pageId
   parent_page_node->Insert(std::make_pair(key,page_bother->GetPageId()),comparator_);
//   更新子兄弟页面的的父页面指针
   page_bother_node->SetParentPageId(parent_page->GetPageId());
   buffer_pool_manager_->UnpinPage(parent_page_id, true);
   return;
}
//父页没有空间，调用父页分裂。先创建一个 父页面的兄弟页面，以供存储父页面分裂出来的元素。
page_id_t page_parent_bother_id;
Page *page_parent_bother = buffer_pool_manager_->NewPage(&page_parent_bother_id);
auto parent_bother_node = reinterpret_cast<InternalPage *>(page_parent_bother->GetData());
parent_bother_node->Init(page_parent_bother_id, INVALID_PAGE_ID, internal_max_size_);
//调用Split函数将page bother中部分分裂出来的元素，同时在Split函数中，page_bother的父页面也会被更新成parent_page_node
//假如在这个例子里面，每一级最多只能两个，那那么插入21 22之后，叶子页面和第二层内部页面都会分裂
parent_page_node->Split(key, page_bother, page_parent_bother, comparator_, buffer_pool_manager_);
//向上递归分裂
InsertInParent(parent_page,parent_page_node->KeyAt(0),page_parent_bother);
buffer_pool_manager_->UnpinPage(page_parent_bother_id, true);
buffer_pool_manager_->UnpinPage(parent_page_id, true);
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction)
{
  if (IsEmpty())
  {
     return ;
  }
  auto leaf_page= FindLeafPage(key);
//  如果没有找到这个leaf page
  if (leaf_page== nullptr)
  {
     return;
  }
  DeleteEntry(leaf_page,key);
}


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DeleteEntry(Page *&page, const KeyType &key) -> void {
  auto b_node = reinterpret_cast<BPlusTreePage *>(page->GetData());
//  尝试删除内部页面或者叶子页面的的某个key
  if (b_node->IsLeafPage()) {
     auto leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
     if (!leaf_node->Delete(key, comparator_)) {
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      return;
     }
  } else {
     auto inter_node = reinterpret_cast<InternalPage *>(page->GetData());
     if (!inter_node->Delete(key, comparator_)) {
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      return;
     }
  }
  if (root_page_id_ == b_node->GetPageId()) {
     AdjustRootPage(b_node);
     return;
  }
//  涉及到分裂
  if (b_node->GetSize() < b_node->GetMinSize()) {
     Page *bother_page;
     KeyType parent_key{};
     bool ispre;
     auto inter_node = reinterpret_cast<InternalPage *>(page->GetData());
//     获取父页面
     auto parent_page_id = inter_node->GetParentPageId();
     auto parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
     auto parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
//     获取page的兄弟页面，逻辑是首先获取page在parent_node中的位置，然后我们可以获得左边或者右边的兄弟页面的page id
     parent_node->GetBotherPage(page->GetPageId(), bother_page, parent_key, ispre, buffer_pool_manager_);
     auto bother_node = reinterpret_cast<BPlusTreePage *>(bother_page->GetData());
//如果b_node在删除页面之后和兄弟页面的KV之和小于等于最大值，那么可以直接合并，同时我们要确保，bother page是在要被合并的页面左侧，如果不是，就直接利用swap函数交换内容。
     if (bother_node->GetSize() + b_node->GetSize() <= GetMaxsize(b_node)) {
      if (!ispre) {
        std::swap(page, bother_page);
      }
//      合并
      Coalesce(page, bother_page, parent_key);
//      向上递归删除
      DeleteEntry(parent_page, parent_key);
     } else {
//      如果尝试合并之后新的页面的KV之和大于能够容纳的最大值，那么就分裂
      Redistribute(page, bother_page, parent_page, parent_key, ispre);
     }
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}
/**
 * 在B+树中重新调整根页面。
 *
 * @tparam INDEX_TEMPLATE_ARGUMENTS 参数列表，包括键的类型、值的类型和键的比较器类型。
 *
 * @param b_node 当前作为根的页面节点。
 *
 * 该函数确保B+树的根始终满足特定条件，或者根被适当地替换或删除，以维持B+树的属性。
 *
 * 1. 如果当前根是叶子页面且其大小为0（即没有包含任何键），该函数会设置无效的页面ID为新的根页面ID，然后释放并删除原根页面。
 *
 * 2. 如果当前根是内部页面且其大小为1（即只有一个子页面），这意味着它只有一个子页面可以作为新的根。此时，该函数将把这个子页面设置为新的根，并释放并删除原根页面。
 *
 * 无论哪种情况，都确保释放（Unpin）和删除了不再需要的页面，以避免浪费资源。
 *
 * 示例：
 * 假设B+树有如下结构：
 *      10
 *     /  \
 *    5   15
 *
 * 如果我们删除键15，那么内部节点只包含一个子节点5，该函数将调整结构，使5成为新的根。
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::AdjustRootPage(BPlusTreePage *b_node) {
  if (b_node->IsLeafPage() && b_node->GetSize() == 0) {
     root_page_id_ = INVALID_PAGE_ID;
     UpdateRootPageId(false);
     buffer_pool_manager_->UnpinPage(b_node->GetPageId(), true);
     buffer_pool_manager_->DeletePage(b_node->GetPageId());
     return;
  }
  if (!b_node->IsLeafPage() && b_node->GetSize() == 1) {
     auto inter_node = reinterpret_cast<InternalPage *>(b_node);
     root_page_id_ = inter_node->ValueAt(0);
     UpdateRootPageId(false);
     buffer_pool_manager_->UnpinPage(b_node->GetPageId(), true);
     buffer_pool_manager_->DeletePage(b_node->GetPageId());
     return;
  }
}


/**
 * 在B+树中合并页面。
 *
 * @tparam INDEX_TEMPLATE_ARGUMENTS 参数列表，包括键的类型、值的类型和键的比较器类型。
 *
 * @param page 需要合并的主要页面。
 * @param bother_page 与主页面合并的兄弟页面。
 * @param parent_key 父页面中的键，与要合并的两个页面关联。
 *
 * 该函数负责合并B+树中的两个页面，无论它们是叶子页面还是内部页面。
 *
 * 如果要合并的页面是叶子页面，它们将直接合并，而合并后的页面的下一个页面ID将设置为被合并页面的下一个页面ID。
 *
 * 如果要合并的页面是内部页面，兄弟页面（作为左页面）将与主页面（作为右页面）以及来自父页面的键进行合并。
 *
 * 合并完成后，右页面将被删除，而左页面则进行了修改。由于页面内容发生了变化，所以它们被标记为"dirty"，
 * 表示在适当的时候需要写回磁盘。在完成所有操作后，函数将释放这两个页面。
 *
 * 示例：
 * 假设B+树有如下结构：
 *      10
 *     /  \
 *    5   15
 *   / \  / \
 *  1  7 12  20
 *
 * 如果页面containing 7和页面containing 12都只有一个键，并且我们删除键7，页面containing 7和12可能需要合并，
 * 结果可能导致B+树结构发生改变。
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Coalesce(bustub::Page *page, bustub::Page *bother_page, const KeyType &parent_key)
{
  auto b_node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  if (b_node->IsLeafPage())
  {
//     合并叶子页面和其兄弟页面
     auto leaf_bother_node = reinterpret_cast<LeafPage *>(bother_page->GetData());
     auto leaf_b_node = reinterpret_cast<LeafPage *>(page->GetData());
     leaf_bother_node->Merge(page, buffer_pool_manager_);
//     合并完了之后，合并之后的页面指向的页面就是被合体的叶子页面指向的页面，也就是A->B->C,A和B结合之后，A->C
     leaf_bother_node->SetNextPageId(leaf_b_node->GetNextPageId());
  }
//  如果是内部页面的话，
  else {
//     bother page作为左侧的页面和右侧的page进行合并
     auto inter_bother_node = reinterpret_cast<InternalPage *>(bother_page->GetData());
     inter_bother_node->Merge(parent_key, page, buffer_pool_manager_);
  }
//  右侧的页面合并之后会被删除，并且都进行了修改，所以dirty都是true
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  buffer_pool_manager_->DeletePage(page->GetPageId());
  buffer_pool_manager_->UnpinPage(bother_page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Redistribute(Page *page, Page *bother_page, Page *parent_page, const KeyType &parent_key,
                                  bool ispre) {
  auto bother_node = reinterpret_cast<BPlusTreePage *>(bother_page->GetData());
  // 如果 bother_node 是内部页
  if (bother_node->IsRootPage()) {
     auto inter_bother_node = reinterpret_cast<InternalPage *>(bother_page->GetData());
     auto inter_b_node = reinterpret_cast<InternalPage *>(page->GetData());
     // 拿出 bother 最后的元素，放到 page 的最前面， child 也需要随之改变
     Page *child_page;
     KeyType key;
     if (ispre) {
      page_id_t last_value = inter_bother_node->ValueAt(inter_bother_node->GetSize() - 1);
      KeyType last_key = inter_bother_node->KeyAt(inter_bother_node->GetSize() - 1);
      inter_bother_node->Delete(last_key, comparator_);
      inter_b_node->InsertFirst(parent_key, last_value);
      child_page = buffer_pool_manager_->FetchPage(last_value);
      key = last_key;
     } else {
      page_id_t first_value = inter_bother_node->ValueAt(0);
      KeyType first_key = inter_bother_node->KeyAt(1);
      inter_bother_node->DeleteFirst();
      inter_b_node->Insert(std::make_pair(parent_key, first_value), comparator_);
      child_page = buffer_pool_manager_->FetchPage(first_value);
      key = first_key;
     }
     auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
     if (child_node->IsLeafPage()) {
      auto leaf_child_node = reinterpret_cast<LeafPage *>(child_page->GetData());
      leaf_child_node->SetParentPageId(inter_b_node->GetPageId());
     } else {
      auto inter_child_node = reinterpret_cast<InternalPage *>(child_page->GetData());
      inter_child_node->SetParentPageId(inter_b_node->GetPageId());
     }
     buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);
     auto inter_parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
     int index = inter_parent_node->KeyIndex(parent_key, comparator_);
     inter_parent_node->SetKeyAt(index, key);
     buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
     buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
     buffer_pool_manager_->UnpinPage(bother_page->GetPageId(), true);
  } else {  // bother 是叶子页
     auto leaf_bother_node = reinterpret_cast<LeafPage *>(bother_page->GetData());
     auto leaf_b_node = reinterpret_cast<LeafPage *>(page->GetData());
     // 拿出 bother 最后的元素，放到 page 的最前面， 没有 child
     KeyType key;
     if (ispre) {
      ValueType last_value = leaf_bother_node->ValueAt(leaf_bother_node->GetSize() - 1);
      KeyType last_key = leaf_bother_node->KeyAt(leaf_bother_node->GetSize() - 1);
      leaf_bother_node->Delete(last_key, comparator_);
      leaf_b_node->InsertFirst(last_key, last_value);
      key = last_key;
     } else {
      ValueType first_value = leaf_bother_node->ValueAt(0);
      KeyType first_key = leaf_bother_node->KeyAt(0);
      leaf_bother_node->Delete(first_key, comparator_);
      leaf_b_node->InsertLast(first_key, first_value);
      key = leaf_bother_node->KeyAt(0);
     }
     auto inter_parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
     int index = inter_parent_node->KeyIndex(parent_key, comparator_);
     inter_parent_node->SetKeyAt(index, key);
     buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
     buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
     buffer_pool_manager_->UnpinPage(bother_page->GetPageId(), true);
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return 0; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub