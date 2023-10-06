///===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <mutex>
#include "common/config.h"

namespace bustub {

// 哈希表记录每个frame的frame information
// 然后每个frame information 都储存了时间截。
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

/**
 * TODO(P1): 完成实现
 *
 * @brief 根据LRU-K策略比较两个帧，以确定哪一个更应被驱逐。
 *
 * 该函数检查两个帧（s和t）的背向k距离，如果帧s应该在帧t之前被驱逐，则返回true；否则返回false。
 * 如果一个帧的历史访问次数少于k次，那么它将被优先考虑驱逐。
 *
 * @param[in] s 第一个帧的frame_id。
 * @param[in] t 第二个帧的frame_id。
 * @return bool 如果帧s比帧t更适合被驱逐，则为true，否则为false。
 */
auto LRUKReplacer::Judge(frame_id_t s, frame_id_t t) -> bool {
  if (hash_[s].time_.size() < k_ && hash_[t].time_.size() == k_) {
    return true;
  }
  if (hash_[s].time_.size() == k_ && hash_[t].time_.size() < k_) {
    return false;
  }
  return hash_[s].time_.front() < hash_[t].time_.front();
}

/**
 * TODO(P1): 完成实现
 *
 * @brief 根据LRU-K策略从Replacer中驱逐最合适的帧。
 *
 * 该函数搜索具有最大背向k距离的帧。
 * 对于历史访问次数少于k次的帧，它们将被优先考虑驱逐。如果有多个帧满足此条件，
 * 那么访问时间最早的帧将被驱逐。成功驱逐帧后，将删除该帧的访问历史。
 *
 * @param[out] frame_id 被驱逐的帧的ID。
 * @return bool 如果成功驱逐了一个帧，则为true，如果没有帧可以被驱逐，则为false。
 */
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool 
{
  std::scoped_lock<std::mutex>lock(latch_);
  *frame_id=-1;
  for(const auto &kv: hash_)
  {
    // 首先判断是否可以驱逐
       if(kv.second.evictable_)
       {
          //将当前的frame和上一个可能被驱逐frame进行比较，判断谁应该被驱逐 
           if(*frame_id==-1 || Judge(kv.first, *frame_id))
           {
              *frame_id=kv.first;
           }
       }
  }
  // 如果选出了可以被驱逐的frame
  if(*frame_id!=-1)
  {
    hash_.erase(*frame_id);
    curr_size_=curr_size_-1;
    return true;
  }
  // 如果没有选出frame那么代表驱逐失败
  return false;

}
/**
 * TODO(P1): 完成实现
 *
 * @brief 记录给定帧的访问并更新其访问历史。
 *
 * 当帧被访问时，该函数会被调用，以记录当前的时间戳并更新帧的访问历史。
 * 如果帧的访问历史已经包含 k 个时间戳，最早的时间戳将被移除。
 * 如果帧之前未被访问且哈希表已满，该函数将不会为新帧添加访问历史。
 *
 * @param[in] frame_id 被访问的帧的ID。
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id) 
{
   std::scoped_lock<std::mutex> lock(latch_);
  //  frame 满了或者没有给定的frame_id
   if (replacer_size_==hash_.size() && hash_.count(frame_id)==0)
   {
       return;
   }
  //  保证一直只有k个时间截，这样，我们永远都可以拿到倒数第k个时间截的时间
    if (hash_[frame_id].time_.size() == k_) 
    {
    hash_[frame_id].time_.pop();
  }
  hash_[frame_id].time_.push(current_timestamp_++);

}
/**
 * TODO(P1): 完成实现
 *
 * @brief 设置给定帧的驱逐状态并根据需要调整 Replacer 的大小。
 *
 * 此函数允许用户设置帧是否可以被驱逐。此外，根据帧的新状态，
 * 它还会相应地调整 Replacer 的大小。例如，如果帧之前是可驱逐的，
 * 被设置为不可驱逐，则 Replacer 的大小会减小。
 *
 * 如果提供的帧ID无效（即不在 Replacer 中），该函数会抛出异常或终止进程。
 *
 * @param[in] frame_id 需要设置驱逐状态的帧的ID。
 * @param[in] set_evictable 布尔值，指示帧是否应该被设置为可驱逐。
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) 
{
  std::scoped_lock<std::mutex> lock(latch_);
  if(hash_.count(frame_id)!=0)
  {
    bool flag=hash_[frame_id].evictable_;
    hash_[frame_id].evictable_ = set_evictable;
    if (!flag && set_evictable)
    {
      curr_size_++;
    }
    else if (flag && !set_evictable) {
      curr_size_--;
    }
  }

}

void LRUKReplacer::Remove(frame_id_t frame_id) 
{
   std::scoped_lock<std::mutex> lock(latch_);
   if (hash_.count(frame_id)==0)
   {
    return;
   }
   if(!hash_[frame_id].evictable_)
   {
    
    throw "Remove a non-evictable frame!";
   }
   hash_.erase(frame_id);
   curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub