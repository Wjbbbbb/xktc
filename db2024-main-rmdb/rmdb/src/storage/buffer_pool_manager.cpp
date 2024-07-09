/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "buffer_pool_manager.h"

/**
 * @description: 从free_list或replacer中得到可淘汰帧页的 *frame_id
 * @return {bool} true: 可替换帧查找成功 , false: 可替换帧查找失败
 * @param {frame_id_t*} frame_id 帧页id指针,返回成功找到的可替换帧id
 */
bool BufferPoolManager::find_victim_page(frame_id_t* frame_id) {
       // Todo:
    // 1 使用BufferPoolManager::free_list_判断缓冲池是否已满需要淘汰页面
    // 1.1 未满获得frame
    // 1.2 已满使用lru_replacer中的方法选择淘汰页面
    // 1 使用BufferPoolManager::free_list_判断缓冲池是否已满需要淘汰页面
    if (!free_list_.empty()) {
        // 1.1 缓冲池未满，直接获得frame
        *frame_id = free_list_.front();
        free_list_.pop_front();
        return true;
    } else {
        // 1.2 缓冲池已满，使用lru_replacer中的方法选择淘汰页面
        if (replacer_->victim(frame_id)) {
            // 成功找到要淘汰的页面
            return true;
        } else {
            // 无法找到要淘汰的页面
            return false;
        }
    }
}

/**
 * @description: 更新页面数据, 如果为脏页则需写入磁盘，再更新为新页面，更新page元数据(data, is_dirty, page_id)和page table
 * @param {Page*} page 写回页指针
 * @param {PageId} new_page_id 新的page_id
 * @param {frame_id_t} new_frame_id 新的帧frame_id
 */
void BufferPoolManager::update_page(Page *page, PageId new_page_id, frame_id_t new_frame_id) {
    // TODO: 实现页面更新逻辑
    // 1. 如果是脏页，写回磁盘，并将dirty标志置为false
    if (page->is_dirty()) {
        disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);
        page->is_dirty_ = false; // 将dirty标志置为false，表示页面已经干净
    }
    
    // 2. 更新page table，将旧页面的映射删除，并添加新页面的映射
    page_table_.erase(page->id_); // 删除旧页面的映射
    page_table_[new_page_id] = new_frame_id; // 添加新页面的映射

    // 3. 重置page的data，并更新page id
    page->reset_memory(); // 清空页面内存数据
    page->id_ = new_page_id; // 更新页面的id
}

/**
 * @description: 从buffer pool获取需要的页。
 *              如果页表中存在page_id（说明该page在缓冲池中），并且pin_count++。
 *              如果页表不存在page_id（说明该page在磁盘中），则找缓冲池victim page，将其替换为磁盘中读取的page，pin_count置1。
 * @return {Page*} 若获得了需要的页则将其返回，否则返回nullptr
 * @param {PageId} page_id 需要获取的页的PageId
 */
Page* BufferPoolManager::fetch_page(PageId page_id) {
    std::scoped_lock lock{latch_};
    
    // 1. 从page_table_中查找目标页
    auto it = page_table_.find(page_id);
    
    // 1.1 若目标页在page_table_中找到，则固定(pin)其所在的frame，并返回目标页。
    if (it != page_table_.end()) {
        frame_id_t frame_id = it->second;
        Page* target_page = &pages_[frame_id];
        target_page->pin_count_++;
        replacer_->pin(frame_id);
        //std::scoped_lock unlock{latch_};
        return target_page;
    } else {
        // 1.2 否则，尝试调用find_victim_page获取一个可用的frame，如果失败则返回nullptr
        frame_id_t frame_id;
        if (!find_victim_page(&frame_id)) {
            //std::scoped_lock unlock{latch_};
            return nullptr;
        }
        
        // 2. 如果获取的可用frame存储的是dirty page，则调用update_page将page写回到磁盘
        Page* target_page = &pages_[frame_id];
        update_page(target_page, page_id, frame_id);
        
        // 3. 调用disk_manager_的read_page将目标页读取到frame中
        disk_manager_->read_page(page_id.fd, page_id.page_no, target_page->data_, PAGE_SIZE);
        
        // 4. 固定目标页，并更新pin_count_
        replacer_->pin(frame_id);
        target_page->pin_count_++;
        
        // 5. 返回目标页
        //std::scoped_lock unlock{latch_};
        return target_page;
    }
    
    //std::scoped_lock unlock{latch_};
    return nullptr;
}

/**
 * @description: 取消固定pin_count>0的在缓冲池中的page
 * @return {bool} 如果目标页的pin_count<=0则返回false，否则返回true
 * @param {PageId} page_id 目标page的page_id
 * @param {bool} is_dirty 若目标page应该被标记为dirty则为true，否则为false
 */
bool BufferPoolManager::unpin_page(PageId page_id, bool is_dirty) {
    // 0. 加锁 latch
    std::scoped_lock lock{latch_};
    
    // 1. 尝试在 page_table_ 中查找 page_id 对应的页 P
    auto it = page_table_.find(page_id);
    
    // 1.1 如果 P 在页表中不存在，则返回 false
    if (it == page_table_.end()) {
        //std::scoped_lock unlock{latch_};
        return false;
    }
    
    // 1.2 如果 P 在页表中存在，则获取其 pin_count_
    frame_id_t frame_id = it->second;
    Page* target_page = &pages_[frame_id];

    // 2.1 如果 pin_count_ 已经等于 0，则返回 false
    if (target_page->pin_count_ <= 0) {
        //std::scoped_lock unlock{latch_};
        return false;
    }
    
    // 2.2 如果 pin_count_ 大于 0，则将 pin_count_ 自减一
    target_page->pin_count_--;
    
    // 2.2.1 如果自减后 pin_count_ 等于 0，则调用 replacer_ 的 Unpin 方法
    if (target_page->pin_count_ == 0) {
        replacer_->unpin(frame_id);
    }
    
    // 3. 根据参数 is_dirty，更新 P 的 is_dirty_ 标志
    target_page->is_dirty_ = is_dirty;
    
    //std::scoped_lock unlock{latch_};
    return true;
}
/**
 * @description: 将目标页写回磁盘，不考虑当前页面是否正在被使用
 * @return {bool} 成功则返回true，否则返回false(只有page_table_中没有目标页时)
 * @param {PageId} page_id 目标页的page_id，不能为INVALID_PAGE_ID
 */
bool BufferPoolManager::flush_page(PageId page_id) {
    // 0. 加锁 latch
    std::scoped_lock lock{latch_};
    
    // 1. 查找页表，尝试获取目标页 P
    auto it = page_table_.find(page_id);

    // 1.1 如果目标页 P 没有在 page_table_ 中记录，返回 false
    if (it == page_table_.end()) {
        //std::scoped_lock unlock{latch_};
        return false;
    }
    
    // 2. 无论 P 是否为脏页，都将其写回磁盘
    frame_id_t frame_id = it->second;
    Page* target_page = &pages_[frame_id];
    disk_manager_->write_page(page_id.fd, page_id.page_no, target_page->data_, PAGE_SIZE);
    
    // 3. 更新 P 的 is_dirty_ 标志为 false
    target_page->is_dirty_ = false;

    //std::scoped_lock unlock{latch_};
    return true;
}
/**
 * @description: 创建一个新的page，即从磁盘中移动一个新建的空page到缓冲池某个位置。
 * @return {Page*} 返回新创建的page，若创建失败则返回nullptr
 * @param {PageId*} page_id 当成功创建一个新的page时存储其page_id
 */
Page* BufferPoolManager::new_page(PageId* page_id) {
    // 0. 加锁 latch
    std::scoped_lock lock{latch_};
    
    frame_id_t frame_id;

    // 1. 获得一个可用的 frame，若无法获得则返回 nullptr
    if (!find_victim_page(&frame_id)) {
        //std::scoped_lock unlock{latch_};
        return nullptr;
    }

    // 2. 在 fd 对应的文件分配一个新的 page_id
    int page_fd = page_id->fd;
    page_id_t page_no = disk_manager_->allocate_page(page_fd);

    if (page_no == INVALID_PAGE_ID) {
        //std::scoped_lock unlock{latch_};
        return nullptr;
    }

    page_id->page_no = page_no;

    // 3. 将 frame 的数据写回磁盘
    Page* new_page = &pages_[frame_id];
    update_page(new_page, *page_id, frame_id);

    // 4. 固定 frame，更新 pin_count_
    replacer_->pin(frame_id);
    new_page->pin_count_ = 1;

    // 5. 返回获得的 page
    //std::scoped_lock unlock{latch_};
    return new_page;
}
/**
 * @description: 从buffer_pool删除目标页
 * @return {bool} 如果目标页不存在于buffer_pool或者成功被删除则返回true，若其存在于buffer_pool但无法删除则返回false
 * @param {PageId} page_id 目标页
 */
bool BufferPoolManager::delete_page(PageId page_id) {
    // 0. 加锁 latch
    std::scoped_lock lock{latch_};
    
    // 1. 在 page_table_ 中查找目标页，若不存在返回 true
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        //std::scoped_lock unlock{latch_};
        return true;
    }
    
    // 2. 若目标页的 pin_count 不为 0，则返回 false
    frame_id_t frame_id = it->second;
    Page* target_page = &pages_[frame_id];
    if (target_page->pin_count_ != 0) {
      //std::scoped_lock unlock{latch_};
        // 目标页的 pin_count != 0
        return false;
    }
    
    // 3. 将目标页数据写回磁盘，从页表中删除目标页，重置其元数据，将其加入 free_list_，返回 true
    disk_manager_->write_page(page_id.fd, page_id.page_no, target_page->data_, PAGE_SIZE);
    page_table_.erase(it);
    target_page->reset_memory();
    free_list_.push_back(frame_id);

    return true;
}
/**
 * @description: 将buffer_pool中的所有页写回到磁盘
 * @param {int} fd 文件句柄
 */
void BufferPoolManager::flush_all_pages(int fd) {
    std::scoped_lock lock{latch_};
    // 1. 遍历页表中的所有页面
    for (auto &it : page_table_) {
        // 2. 检查文件描述符是否与指定fd匹配
        if (it.first.fd != fd) {
            continue; // 如果不匹配，则跳过当前页面
        }
        Page *page = &pages_[it.second];
        // 3. 将脏页数据写回磁盘
        disk_manager_->write_page(fd, page->id_.page_no, page->data_, PAGE_SIZE);
        // 4. 清除页面的脏标记
        page->is_dirty_ = false;
    }
    // 无需在此处解锁，因为std::scoped_lock会在作用域结束时自动解锁
}