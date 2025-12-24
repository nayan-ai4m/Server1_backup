/* Copyright 2019-2020 Baumer Optronic */
#include <iostream>
#include <sstream>

#include "DoubleBufferHandler.h"
#include "BufferInformation.h"

DoubleBufferHandler::DoubleBufferHandler()
: buffer_read_(nullptr)
, buffer_write_(nullptr)
, new_data(false) {
}

DoubleBufferHandler::~DoubleBufferHandler() {
}

void DoubleBufferHandler::PushBuffer(BGAPI2::Buffer *buffer) {
    std::lock_guard<std::mutex> lock(buffer_exchange_lock_);
    if (!buffer_write_) {
        buffer_write_ = buffer;
        new_data = true;
    } else {
        buffer->QueueBuffer();
    }
}

BGAPI2::Buffer* DoubleBufferHandler::PullBuffer() {
    std::lock_guard<std::mutex> lock(buffer_exchange_lock_);
    if (buffer_write_) {
        if (buffer_read_) {
            buffer_read_->QueueBuffer();
        }
        buffer_read_ = buffer_write_;
        buffer_write_ = nullptr;
        new_data = false;
    }
    return buffer_read_;
}

void DoubleBufferHandler::FreeBuffer(BGAPI2::Buffer* /*buffer*/) {
    std::lock_guard<std::mutex> lock(buffer_exchange_lock_);
    if (buffer_read_ && buffer_write_) {
        buffer_read_->QueueBuffer();
        buffer_read_ = nullptr;
    }
}

void DoubleBufferHandler::Init() {
    if (buffer_write_) {
        buffer_write_->QueueBuffer();
        buffer_write_ = nullptr;
    }
    if (buffer_read_) {
        buffer_read_->QueueBuffer();
        buffer_read_ = nullptr;
    }
    new_data = false;
}

bool DoubleBufferHandler::HasNewData() {
    return new_data;
}

DoubleBufferHandler::DoubleBufferHandler(const DoubleBufferHandler &instance) {
    buffer_read_ = instance.buffer_read_;
    buffer_write_ = instance.buffer_write_;
}

DoubleBufferHandler & DoubleBufferHandler::operator =(const DoubleBufferHandler &instance) {
    buffer_read_ = instance.buffer_read_;
    buffer_write_ = instance.buffer_write_;
    return *this;
}
