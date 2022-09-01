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

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  // Make sure you call DiskManager::WritePage!
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end() || page_id == INVALID_PAGE_ID) {
    latch_.unlock();
    return false;
  }

  frame_id_t flush_fid = iter->second;
  disk_manager_->WritePage(page_id, pages_[flush_fid].data_);
  
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (auto& p : page_table_) {
      if (pages_[p.second].page_id_ != INVALID_PAGE_ID) {
          disk_manager_->WritePage(p.first, pages_[p.second].data_);
      }
  }
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
    latch_.lock();
  // 0.
  page_id_t new_page_id = AllocatePage();
  // 1.
  bool is_all = true;
  for (int i = 0; i < static_cast<int>(pool_size_); i++) {
    if (pages_[i].pin_count_ == 0) {
      is_all = false;
      break;
    }
  }
  if (is_all) {
    latch_.unlock();
    return nullptr;
  }
  // 2.
  frame_id_t victim_fid;
  if (!find_replace(&victim_fid)) {
    latch_.unlock();
    return nullptr;
  }
  // 3.
  Page *victim_page = &pages_[victim_fid];
  victim_page->page_id_ = new_page_id;
  victim_page->pin_count_++;
  replacer_->Pin(victim_fid);
  page_table_[new_page_id] = victim_fid;
  victim_page->is_dirty_ = false;
  *page_id = new_page_id;
  // [attention]
  // if this not write to disk directly
  // maybe meet below case:
  // 1. NewPage
  // 2. unpin(false)
  // 3. 由于其他页的操作导致该页被从buffer_pool中移除
  // 4. 这个时候在FetchPage， 就拿不到这个page了。
  // 所以这里先把它写回磁盘

  //  disk_manager_->WritePage(victim_page->GetPageId(), victim_page->GetData());

  latch_.unlock();
  return victim_page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
 latch_.lock();
  std::unordered_map<page_id_t, frame_id_t>::iterator it = page_table_.find(page_id);
  // 1.1 P exists
  if (it != page_table_.end()) {
    frame_id_t frame_id = it->second;
    Page *page = &pages_[frame_id];

    //
    page->pin_count_++;        // pin the page
    replacer_->Pin(frame_id);  // notify replacer

    latch_.unlock();
    return page;
  }
  // 1.2 P not exist
  frame_id_t replace_fid;
  if (!find_replace(&replace_fid)) {
    latch_.unlock();
    return nullptr;
  }
  Page *replacePage = &pages_[replace_fid];
  // 2. write it back to the disk
  if (replacePage->IsDirty()) {
    disk_manager_->WritePage(replacePage->page_id_, replacePage->data_);
  }
  // 3
  page_table_.erase(replacePage->page_id_);
  // create new map
  // page_id <----> replaceFrameID;
  page_table_[page_id] = replace_fid;
  // 4. update replacePage info
  Page *newPage = replacePage;
  disk_manager_->ReadPage(page_id, newPage->data_);
  newPage->page_id_ = page_id;
  newPage->pin_count_++;
  newPage->is_dirty_ = false;
  replacer_->Pin(replace_fid);
  latch_.unlock();

  return newPage;

}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  latch_.lock();

  // 1.
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return true;
  }
  // 2.
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  if (page->pin_count_ > 0) {
    latch_.unlock();
    return false;
  }
  if (page->is_dirty_) {
    FlushPgImp(page_id);
  }
  // delete in disk in here
  DeallocatePage(page_id);
  
  page_table_.erase(page_id);
  // reset metadata
  page->is_dirty_ = false;
  page->pin_count_ = 0;
  page->page_id_ = INVALID_PAGE_ID;
  // return it to the free list
  
  free_list_.push_back(frame_id);
  latch_.unlock();
  return true;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  latch_.lock();
  // 1. 如果page_table中就没有
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    latch_.unlock();
    return false;
  }
  // 2. 找到要被unpin的page
  frame_id_t unpinned_Fid = iter->second;
  Page *unpinned_page = &pages_[unpinned_Fid];
  if (is_dirty) {
    unpinned_page->is_dirty_ = true;
  }
  // if page的pin_count == 0 则直接return
  if (unpinned_page->pin_count_ == 0) {
    latch_.unlock();
    return false;
  }
  unpinned_page->pin_count_--;
  if (unpinned_page->GetPinCount() == 0) {
    replacer_->Unpin(unpinned_Fid);
  }
  latch_.unlock();
  return true;    
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

bool BufferPoolManagerInstance::find_replace(frame_id_t *frame_id) {
  // if free_list not empty then we don't need replace page
  // return directly
  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
    return true;
  }
  // else we need to find a replace page
  if (replacer_->Victim(frame_id)) {
    // Remove entry from page_table
    int replace_page_id = -1;
    for (const auto &p : page_table_) {
      page_id_t pid = p.first;
      frame_id_t fid = p.second;
      if (fid == *frame_id) {
        replace_page_id = pid;
        break;
      }
    }
    if (replace_page_id != -1) {
      Page *replace_page = &pages_[*frame_id];

      // If dirty, flush to disk
      if (replace_page->is_dirty_) {
        char *data = replace_page->data_;
        disk_manager_->WritePage(replace_page->page_id_, data);
        replace_page->pin_count_ = 0;  // Reset pin_count
      }
      page_table_.erase(replace_page->page_id_);
    }

    return true;
  }

  return false;
}
}  // namespace bustub
