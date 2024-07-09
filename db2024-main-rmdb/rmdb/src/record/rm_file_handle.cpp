/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_file_handle.h"

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid& rid, Context* context) const {
    // 1. 获取指定记录所在的页面句柄
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);

    // 检查页面句柄是否有效
    if (page_handle.page == nullptr) {
        return nullptr; // 如果页面句柄无效，返回nullptr
    }

    // 检查记录号是否在有效范围内
    if (rid.slot_no >= file_hdr_.num_records_per_page || rid.slot_no < 0) {
        return nullptr; // 如果记录号超出范围，返回nullptr
    }

    // 创建一个带有记录大小的unique_ptr<RmRecord>对象
    auto record = std::make_unique<RmRecord>(file_hdr_.record_size);

    // 从页面中复制记录数据到RmRecord对象
    std::memcpy(record->data, page_handle.get_slot(rid.slot_no), file_hdr_.record_size);
    record->size = file_hdr_.record_size;

    return record;
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid RmFileHandle::insert_record(char* buf, Context* context) {
    // 1. 获取当前页面句柄，确保页面未满
    RmPageHandle page_handle = create_page_handle();

    int slot_no = -1;
    // 2. 查找空闲槽位位置
    for (slot_no = 0; slot_no < file_hdr_.num_records_per_page; slot_no++) {
        if (!Bitmap::is_set(page_handle.bitmap, slot_no)) {
            // 找到空闲槽位
            break;
        }
    }

    if (slot_no == file_hdr_.num_records_per_page) {
        // 页面已满，无法插入
        return Rid{-1, -1};
    }

    // 3. 将数据复制到空闲槽位
    std::memcpy(page_handle.get_slot(slot_no), buf, file_hdr_.record_size);
    Bitmap::set(page_handle.bitmap, slot_no);

    // 4. 更新页面头部信息
    page_handle.page_hdr->num_records++;

    // 更新file_hdr_.first_free_page_no，如果页面满了
    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) {
        file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
    }

    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);

    return Rid{page_handle.page->get_page_id().page_no, slot_no};
}

/**
 * @description: 在当前表中的指定位置插入一条记录
 * @param {Rid&} rid 要插入记录的位置
 * @param {char*} buf 要插入记录的数据
 */
void RmFileHandle::insert_record(const Rid& rid, char* buf) {
    // 获取页面句柄
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);

    // 如果页面为空或者插入的槽位号超出页面容量，则直接返回
    if (page_handle.page == nullptr || rid.slot_no >= file_hdr_.num_records_per_page) {
        return;
    }

    // 获取插入位置的槽位指针并复制数据
    char* slot = page_handle.get_slot(rid.slot_no);
    std::memcpy(slot, buf, file_hdr_.record_size);

    // 解锁页面并将其标记为已修改
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {Rid&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
void RmFileHandle::delete_record(const Rid& rid, Context* context) {
    // 获取指定记录所在的页面句柄
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);

    // 如果页面为空或者槽位号超出页面容量，则直接返回
    if (page_handle.page == nullptr || rid.slot_no >= file_hdr_.num_records_per_page) {
        return;
    }

    // 重置位图，表示槽位空闲
    Bitmap::reset(page_handle.bitmap, rid.slot_no);
    // 更新页面记录数
    page_handle.page_hdr->num_records--;

    // 解锁页面并标记为已修改
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);

    // 如果删除记录后页面未满，释放页面句柄
    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page - 1) {
        release_page_handle(page_handle);
    }
}


/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {Rid&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context
 */
void RmFileHandle::update_record(const Rid& rid, char* buf, Context* context) {
    // 获取指定记录所在的页面句柄
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);

    // 如果页面为空或者槽位号超出页面容量，则直接返回
    if (page_handle.page == nullptr || rid.slot_no >= file_hdr_.num_records_per_page) {
        return;
    }

    // 更新记录数据
    char* slot = page_handle.get_slot(rid.slot_no);
    std::memcpy(slot, buf, file_hdr_.record_size);

    // 解锁页面并标记为已修改
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

/**
 * 以下函数为辅助函数，仅提供参考，可以选择完成如下函数，也可以删除如下函数，在单元测试中不涉及如下函数接口的直接调用
*/
/**
 * @description: 获取指定页面的页面句柄
 * @param {int} page_no 页面号
 * @return {RmPageHandle} 指定页面的句柄
 */
RmPageHandle RmFileHandle::fetch_page_handle(int page_no) const {
    // 使用缓冲池获取指定页面，并生成页面句柄返回给上层调用者

    // 如果页号无效，抛出异常（当前实现中暂未处理异常）
    if (page_no < 0 || page_no >= file_hdr_.num_pages) {
        // TODO: 抛出异常，当前不处理表名信息
        return RmPageHandle(&file_hdr_, nullptr);
    }

    // 从缓冲池中获取页面
    Page* page = buffer_pool_manager_->fetch_page(PageId{ fd_, page_no });

    // 如果获取失败，返回空的页面句柄
    if (page == nullptr) {
        // 调用fetch_page获取失败
        return RmPageHandle(&file_hdr_, nullptr);
    }

    // 返回带有页面头和页面指针的页面句柄
    return RmPageHandle(&file_hdr_, page);
}

/**
 * @description: 创建一个新的page handle
 * @return {RmPageHandle} 新的PageHandle
 */
RmPageHandle RmFileHandle::create_new_page_handle() {
    // 使用缓冲池创建一个新页面

    // 准备新页面的页号，暂时设置为无效页号
    auto page_id = PageId{ fd_, -1 };
    Page* page = buffer_pool_manager_->new_page(&page_id);

    // 如果创建失败，返回空的页面句柄
    if (page == nullptr) {
        // 创建失败
        return RmPageHandle(&file_hdr_, nullptr);
    }

    // 创建页面句柄并更新相关信息
    RmPageHandle page_handle = RmPageHandle(&file_hdr_, page);
    page_handle.page_hdr->next_free_page_no = RM_NO_PAGE;
    page_handle.page_hdr->num_records = 0;

    // 更新文件头的页面数
    file_hdr_.num_pages++;

    // 返回新创建的页面句柄
    return page_handle;
}

/**
 * @brief 创建或获取一个空闲的page handle
 *
 * @return RmPageHandle 返回生成的空闲page handle
 * @note pin the page, remember to unpin it outside!
 */
RmPageHandle RmFileHandle::create_page_handle() {
    // 判断文件头中是否还有空闲页
    //   如果没有空闲页：使用缓冲池创建一个新页面，直接调用create_new_page_handle()
    //   如果有空闲页：直接获取第一个空闲页

    if (file_hdr_.first_free_page_no == RM_NO_PAGE) {
        // 没有空闲页，创建一个新页面句柄并返回
        return create_new_page_handle();
    } else {
        // 有空闲页，获取第一个空闲页的页面句柄并返回
        return fetch_page_handle(file_hdr_.first_free_page_no);
    }
}
/**
 * @description: 当一个页面从没有空闲空间的状态变为有空闲空间状态时，更新文件头和页头中空闲页面相关的元数据
 */
void RmFileHandle::release_page_handle(RmPageHandle& page_handle) {
    // 当页面从已满变成未满时，更新相关信息：

    // 更新页面头中的下一个空闲页面号为文件头中当前的第一个空闲页号
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;

    // 更新文件头中的第一个空闲页号为当前释放页面的页号
    file_hdr_.first_free_page_no = page_handle.page->get_page_id().page_no;
}