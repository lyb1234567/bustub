//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <list>
#include <mutex>
#include <tuple>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
: global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) 
    {
      dir_.emplace_back(std::make_shared<Bucket>(bucket_size));

    }

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index=this->IndexOf(key);
  auto ans = dir_[index]->Find(key, value);
  return ans;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index=this->IndexOf(key);
  auto ans=dir_[index]->Remove(key);
  return ans;
}

/**
 * @brief 重新分配溢出的桶中的元素，创建一个新的桶，并在必要时更新目录条目。
 * 
 * 当一个桶满了并且需要插入新的元素时，我们需要创建一个新的桶并调整两个桶的内容。
 * 函数首先增加原桶的局部深度，并创建一个新的桶。然后，根据每个键的新哈希值，
 * 将原桶中的某些元素移动到新桶。最后，根据新的局部深度更新目录条目，以确保它们指向
 * 正确的桶。
 * 
 * @param bucket 溢出的桶的智能指针，需要重新分配的桶。
 * 
 * @return void
 */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void
{
  bucket->IncrementDepth();
  int depth=bucket->GetDepth();
  num_buckets_++;
  // 建一个新桶子
  std::shared_ptr<Bucket> p(new Bucket(bucket_size_, depth));
  // 分裂前原始的桶子中元素对应的index，比如说，在桶子装满前，原始的桶子中有三个元素，21 25 29，他们对应的index都是一个
  // 分裂之后，local depth需要加一，这样我们重新计算每一个桶子里的元素，保持一致的就留着，不一样的，就放到分裂的新桶里。
   size_t preidx = std::hash<K>()((*(bucket->GetItems().begin())).first) & ((1 << (depth - 1)) - 1);
   for(auto it=bucket->GetItems().begin();it!=bucket->GetItems().end();it++ )
   {
    //  新的index
     size_t idx=std::hash<K>()((*it).first)& ((1 << (depth - 1)) - 1);
     if (idx!=preidx)
     {
      // 如果是新的元素，那么就插入新的桶子里
       p->Insert((*it).first, (*it).second);
      //  插入分裂的新桶子的元素在原来的桶子就被删除
       bucket->GetItems().erase(it++);
     }
     else
     {
      it++;
     }
    //  新桶分裂新建完毕之后，我们就需要考虑，dir中哪些条目的指针是指向新桶子，哪些还是保留原来的位置
    // 需要满足两个条件：1.低 depth-1位和predix想匹配 2.低depth位和predix不匹配
    // 因为当一个桶子满了之后，分裂之后，我们需要会增加局部深度，比如原来是2，现在会变成3，那么新的原本最后2位其实依旧一致，但是第三位会不一样
    // 比如 当局部深度为2时：101 001都是一样的， 但是局部深度为3时： 101 001是不一样的
     for(size_t i=0;i<dir_.size();i++)
     {
      if((i & ((1 << (depth - 1)) - 1)) == preidx && (i & ((1 << depth) - 1)) != preidx)
      {
        dir_[i]=p;
      }
     }

   }
   
}

/**
 * @brief 将给定的键值对插入扩展哈希表中。
 * 
 * 该函数首先锁定哈希表以确保线程安全。然后，它尝试将键值对插入计算出的目录索引对应的桶中。
 * 如果插入成功，则退出。如果插入失败（通常是因为桶已满），则进行以下处理：
 * 1. 如果当前桶的局部深度小于全局深度，则仅重新分配该桶。
 * 2. 如果局部深度等于全局深度，则增加全局深度并将目录大小加倍。
 * 
 * @param key   要插入的键。
 * @param value 要插入的值。
 * 
 * @return void
 */
template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  // UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);
  while (true) {
    size_t index = this->IndexOf(key);
    bool flag = dir_[index]->Insert(key, value);
    // 先判断插入是否成功，如果成功直接退出
    if (flag) {
      break;
    }
    if (GetLocalDepthInternal(index) != GetGlobalDepthInternal()) {
      RedistributeBucket(dir_[index]);
    } else {
      global_depth_++;
      size_t dir_size = dir_.size();
      for (size_t i = 0; i < dir_size; i++) {
        // 新增加的entry链接到对应的bucke，扩容之后，会有多出来的新条目等待分配指针，这个时候依旧在循环中
        // 在redistribute()函数中，会为分裂出来的桶子分配新的指针。
        dir_.emplace_back(dir_[i]);
      }
    }
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//

/**
 * @brief 在桶中查找给定键的键值对，并将对应的值存储在提供的变量中。
 * 
 * 该函数遍历桶的内部列表，查找与给定键匹配的键值对。如果找到，它会将该键值对的值
 * 存储在提供的'value'变量中，并返回 true。如果未找到匹配的键，则返回 false。
 * 
 * @param key   需要查找的键。
 * @param value 用于存储找到的键对应的值的引用。
 * 
 * @return 如果在桶中找到了与给定键匹配的键值对，返回 true；否则返回 false。
 */
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for(auto it=list_.begin();it!=list_.end();it++)
  {
    if((*it).first==key)
    {
      (*it).second=value;
      return true;
    }
  }
  return false;
}


/**
 * @brief 从桶中根据提供的键删除键值对。
 * 
 * 该函数遍历桶内部的列表，以查找与给定键对应的键值对。如果找到，则从列表中删除该对。
 * 
 * @param key 需要被删除的键值对的键。
 * 
 * @return 如果找到并删除了键值对，则返回 true；否则返回 false。
 * 
 * @note 删除一个键值对时，指向它的迭代器会失效。此函数在删除操作中通过后自增迭代器来安全地处理迭代器失效的问题。
 */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for(auto it=list_.begin();it!=list_.end();)
  {
    if((*it).first==key)
    {
      list_.erase(it++);
      return true;
    }
    it++;
  }
  return false;
}

/**
 * @brief 插入或更新桶中的键值对。
 * 
 * 此函数首先搜索与给定键匹配的键。如果找到匹配项，则更新对应的值。
 * 如果没有找到匹配项并且桶还没有满，则插入新的键值对。
 * 
 * @param key 要插入或更新的键。
 * @param value 与键相关的值。
 * 
 * @return 返回 true 如果插入或更新成功；否则返回 false。
 * 
 * @note 如果键已存在，此函数将更新其值。如果桶已满，则无法插入新的键值对。
 */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for(auto it=list_.begin();it!=list_.end();it++)
  {
    if ((*it).first==key)
    {
      (*it).second=value;
      return true;
    }
  }
  if(this->IsFull())
  {
    return false;
  }
  list_.emplace_back(std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub