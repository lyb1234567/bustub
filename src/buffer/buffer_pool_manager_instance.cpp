//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include <mutex>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}
/**
 * @brief 在缓冲池中分配一个新页面。
 * 
 * 该函数管理缓冲池中新页面的分配。如果`free_list_`中有可用的空闲帧，它将使用该帧。
 * 否则，它尝试使用替换器驱逐一个帧。如果驱逐了一个帧并且它是"dirty"状态的，该函数确保页面数据被写回磁盘。
 * 成功找到或创建帧后，该函数初始化新页面并更新相关元数据。
 * 
 * @param[out] page_id 用于存储分配页面的ID的指针。
 * 
 * @return 指向缓冲池中新分配的页面的指针。如果分配失败，则返回nullptr。
 * 
 * @note 由于在`latch_`上的作用域锁，此函数是线程安全的。
 */
auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * 
{ 
  std::scoped_lock<std::mutex>lock(latch_);
  frame_id_t frame_id=-1;
  // 从free_list中寻找没有在使用的frame_id，因为free_list中，所有的frame_id都是未使用的，所以，如果free_list非空
  // 直接用第一个free_lst中第一个frame_id即可
  if (!free_list_.empty())
  {
     frame_id=free_list_.front();
     free_list_.pop_front();
  }
  // 如果free list中没有可用的frame_id，那我们就尝试用replacer驱逐当前的frame_id，看看是否可以，如果可以，那么就直接使用
  else if(replacer_ -> Evict(&frame_id))
  {
    // 如果当前的page是dirty的话，先写入磁盘
      if (pages_[frame_id].IsDirty())
      {
         disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
      }
      page_table_->Remove(pages_[frame_id].GetPageId());
  }
  else
  {
    page_id=nullptr;
    return nullptr;
  }
  *page_id = AllocatePage();
  // 假设您想知道一个特定的 page_id 是否已经被加载到缓冲池中。
  //你不能直接用 page_id 去索引 pages_ 数组，因为 page_id 和 frame_id 之间没有直接的关系。
  //但是，通过哈希表，您可以快速地查找给定的 page_id 对应的 frame_id。
  page_table_->Insert(*page_id, frame_id);
  pages_[frame_id].ResetMemory();
  // 这样的话，我们就可以保证，在可扩展哈希表中，我们可以通过page_id知道对应的frame_id及buffer pool的位置
  // 同时在buffer pool中，我们也可以直接通过frame_id锁定对应的page
  pages_[frame_id].page_id_=*page_id;
  // 将dirty_flag 和pin_count设置为1
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_=false;
  // 记录给定帧的访问并更新其访问历史
  replacer_->RecordAccess(frame_id);
  // 因为pin为1那么目前的frame就是不可驱逐的。
  replacer_->SetEvictable(frame_id, false);
  // 返回特定的生成的page
  return &pages_[frame_id];

}
/**
 * @brief 根据给定的page_id获取缓冲池中的页面。
 * 
 * 这个函数首先检查请求的page_id是否已经在缓冲池中。如果是，则返回该页面并更新相关的元数据（例如，pin_count和访问记录）。
 * 如果页面不在缓冲池中，它将寻找一个可以使用的帧，可能需要驱逐一个当前的帧。然后，从磁盘中读取请求的页面到选定的帧，
 * 并返回对应的页面。
 * 
 * @param page_id 要获取的页面的ID。
 * 
 * @return 指向请求的Page的指针。如果没有可用的帧且无法驱逐任何帧，则返回nullptr。
 * 
 * @note 这个函数是线程安全的，因为它使用了`latch_`上的作用域锁。
 */
auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * { 

// 判断page_id是否合法
  assert(page_id!=INVALID_PAGE_ID);
std::scoped_lock<std::mutex>lock(latch_);
frame_id_t frame_id=-1;
// 首先寻找page_id看看是否已经加载到buffer pool里了，如果有，直接返回特定的page
// 并且遵从访问frame的规则，记录访问，同时pin_count+1
// 同时由于当前的frame要被访问，所以被设置为不可驱逐
if( page_table_->Find(page_id, frame_id))
{
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  pages_[frame_id].pin_count_++;
  return &pages_[frame_id];
}
// 否则就找出可以驱逐的frame,遵循上述操作。
if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Evict(&frame_id)) {
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    }
    page_table_->Remove(pages_[frame_id].GetPageId());
  } else {
    return nullptr;
  }
  // 遵守上述操作，同时磁盘读取特定page中的数据
  page_table_->Insert(page_id, frame_id);
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return &pages_[frame_id];
 }

/**
 * @brief 将缓冲池中的页面解锁（unpin）。
 * 
 * 当一个页面不再被某个事务或操作使用时，它可以被"unpin"。这个函数通过减少页面的pin_count来实现。
 * 如果页面的pin_count达到0，意味着页面现在是可以被驱逐的。此外，这个函数还可以根据参数设置页面的"dirty"标志。
 * 
 * @param page_id 要解锁的页面的ID。
 * @param is_dirty 如果为true，页面将被标记为"dirty"。
 * 
 * @return 如果成功解锁返回true，如果页面未被pin或找不到页面则返回false。
 * 
 * @note 这个函数是线程安全的，因为它在操作过程中使用了`latch_`上的作用域锁。
 */

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool 
{ 
   std::scoped_lock<std::mutex> lock(latch_);
   frame_id_t frame_id;
  //  如果page没有被加载到buffer pool中，或者当前在buffer pool中page本身就没有pin，就不用Unpin
   if (!page_table_->Find(page_id, frame_id) || pages_[frame_id].GetPinCount() == 0) {
    return false;
  }
  pages_[frame_id].pin_count_--;
  // 用于判断当前这个页面是否已经被修改但是未写入（dirty).
  pages_[frame_id].is_dirty_ |= is_dirty;
  if (pages_[frame_id].GetPinCount()==0)
  {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
 }

/**
 * @brief 将缓冲池中的指定页面写回到磁盘。
 * 
 * 如果页面在缓冲池中被修改（即标记为"dirty"），它的内容会与磁盘上的版本不同步。此函数确保
 * 将指定的页面内容从缓冲池刷新（写回）到磁盘，使其与磁盘上的版本同步。
 * 
 * @param page_id 要刷新到磁盘的页面的ID。
 * 
 * @return 如果页面成功写回到磁盘返回true，如果页面不在页面表中或其他错误则返回false。
 * 
 * @note 这个函数应该在任何需要确保页面持久化到磁盘的情境下调用，例如在事务提交时或在定期检查点操作中。
 */
auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool 
{ 
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  // 如果在buffer中没有找到page，则直接返回false.
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  // 否则写入磁盘，不管是否是dirty page
  disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
  pages_->is_dirty_ = false;
  return true;

}

/**
 * @brief 将缓冲池中的所有页面同步写回到磁盘。
 * 
 * 该函数遍历整个缓冲池，检查每个页面是否已经加载到缓冲池中（即是否在页面表中）。
 * 对于已加载的页面，它会将其内容写回到磁盘，并将该页面的"dirty"标记重置为false。
 * 这确保了缓冲池中的数据与磁盘上的数据保持同步。
 * 
 * @note 该函数通常在需要确保缓冲池中的所有更改都已持久化到磁盘的关键操作中调用，
 * 如在数据库检查点操作或在关闭数据库时。
 */

void BufferPoolManagerInstance::FlushAllPgsImp() 
{
  frame_id_t tmp;
  std::scoped_lock<std::mutex> lock(latch_);
  // 遍历buffer pool中所有page，将当前的page 同步到磁盘中，并且重置dirty
  for (size_t frame_id = 0; frame_id < pool_size_; frame_id++) {
    if (page_table_->Find(pages_[frame_id].GetPageId(), tmp)) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
      pages_->is_dirty_ = false;
    }
  }
}
/**
 * @brief 从缓冲池中删除指定的页面。
 * 
 * 当页面不再需要在缓冲池中时，该函数会被调用。首先，该函数会释放与页面关联的资源，
 * 如磁盘上的页面。接着，如果页面在缓冲池中被标记为"dirty"，它会将页面的内容写回磁盘，
 * 然后从缓冲池中删除页面。此外，页面还会从替换策略（如LRU）中移除，并将其放回到空闲列表中，
 * 以备后续使用。最后，页面会从页面表中移除。
 * 
 * @param page_id 要从缓冲池中删除的页面的ID。
 * 
 * @return 如果页面成功从缓冲池中删除则返回true，如果页面在缓冲池中仍然被pin或其他错误则返回false。
 * 
 * @note 在某些情况下，如果页面在缓冲池中仍然被pin（即其pin计数大于0），那么这个页面不能被删除。
 */
auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool 
{
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  DeallocatePage(page_id);
  if (!page_table_->Find(page_id, frame_id))
  {
    return true;
  }
  if (pages_[frame_id].GetPinCount()>0)
  {
    return false;
  }
  // 如果当前页面是dirty的，代表它在buffer pool中被修改了并且还未被写入磁盘，所以不能删除
  if (pages_[frame_id].IsDirty())
  {
    disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    pages_->is_dirty_ = false;
  }
  // replace中remove掉某个frame_id
  replacer_->Remove(frame_id);
  // 将空闲的frame_id重新放入free list中
  free_list_.push_back(frame_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }
}  // namespace bustub
