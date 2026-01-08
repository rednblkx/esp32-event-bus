#include "event_bus.hpp"
#include "esp_log.h"
#include <algorithm>
#include <cstring>

namespace EventBus {

Bus &Bus::instance() {
  static Bus instance;
  return instance;
}

Bus::Bus() = default;

Bus::~Bus() { stop(); }

int Bus::find_topic_index_by_id(TopicHandle topic_id) const {
  for (size_t i = 0; i < MAX_TOPICS; ++i) {
    if (topics_[i].active && topics_[i].id == topic_id) {
      return static_cast<int>(i);
    }
  }
  return -1;
}

int Bus::find_topic_index_by_name(std::string_view name) const {
  for (size_t i = 0; i < MAX_TOPICS; ++i) {
    if (topics_[i].active && topics_[i].get_name() == name) {
      return static_cast<int>(i);
    }
  }
  return -1;
}

esp_err_t Bus::init() {
  if (running_) {
    ESP_LOGW(TAG, "Event bus already initialized");
    return ESP_ERR_INVALID_STATE;
  }

  mutex_ = xSemaphoreCreateRecursiveMutex();
  if (mutex_ == nullptr) {
    ESP_LOGE(TAG, "Failed to create mutex");
    return ESP_ERR_NO_MEM;
  }

  event_queue_ = xQueueCreate(EVENT_QUEUE_SIZE, sizeof(InternalEvent));
  if (event_queue_ == nullptr) {
    ESP_LOGE(TAG, "Failed to create event queue");
    vSemaphoreDelete(mutex_);
    mutex_ = nullptr;
    return ESP_ERR_NO_MEM;
  }

  topics_.fill(TopicEntry{});
  for (auto &topic_subs : subscribers_) {
    topic_subs.fill(Subscriber{});
  }
  next_topic_id_ = 0;
  running_ = true;

  BaseType_t result =
      xTaskCreate(event_task, "event_bus", EVENT_TASK_STACK_SIZE, this,
                  EVENT_TASK_PRIORITY, &event_task_handle_);

  if (result != pdPASS) {
    ESP_LOGE(TAG, "Failed to create event bus task");
    vQueueDelete(event_queue_);
    vSemaphoreDelete(mutex_);
    event_queue_ = nullptr;
    mutex_ = nullptr;
    running_ = false;
    return ESP_ERR_NO_MEM;
  }

  ESP_LOGI(TAG, "Event bus initialized successfully");
  return ESP_OK;
}

TopicHandle Bus::register_topic(std::string_view topic_name) {
  if (topic_name.empty()) {
    ESP_LOGE(TAG, "Invalid topic name");
    return INVALID_TOPIC;
  }

  if (topic_name.length() >= MAX_TOPIC_NAME_LEN) {
    ESP_LOGE(TAG, "Topic name too long (max %zu chars)",
             MAX_TOPIC_NAME_LEN - 1);
    return INVALID_TOPIC;
  }

  if (xSemaphoreTakeRecursive(mutex_, pdMS_TO_TICKS(100)) != pdTRUE) {
    ESP_LOGE(TAG, "Failed to acquire mutex");
    return INVALID_TOPIC;
  }

  int existing = find_topic_index_by_name(topic_name);
  if (existing >= 0) {
    TopicHandle existing_id = topics_[existing].id;
    xSemaphoreGiveRecursive(mutex_);
    ESP_LOGW(TAG, "Topic '%.*s' already registered with ID %lu",
             static_cast<int>(topic_name.length()), topic_name.data(),
             existing_id);
    return existing_id;
  }

  auto it = std::find_if(topics_.begin(), topics_.end(),
                         [](const TopicEntry &t) { return !t.active; });

  if (it == topics_.end()) {

    xSemaphoreGiveRecursive(mutex_);
    ESP_LOGE(TAG, "No available topic slots (max %zu)", MAX_TOPICS);
    return INVALID_TOPIC;
  }

  TopicHandle new_id = next_topic_id_++;
  it->set_name(topic_name);
  it->id = new_id;
  it->active = true;

  xSemaphoreGiveRecursive(mutex_);

  ESP_LOGD(TAG, "Topic registered: '%.*s' (ID: %lu)",
           static_cast<int>(topic_name.length()), topic_name.data(), new_id);
  return new_id;
}

std::optional<TopicHandle> Bus::get_topic(std::string_view topic_name) const {
  if (topic_name.empty()) {
    return std::nullopt;
  }

  if (xSemaphoreTakeRecursive(mutex_, pdMS_TO_TICKS(100)) != pdTRUE) {
    ESP_LOGE(TAG, "Failed to acquire mutex");
    return std::nullopt;
  }

  int index = find_topic_index_by_name(topic_name);
  std::optional<TopicHandle> result = std::nullopt;

  if (index >= 0) {
    result = topics_[index].id;
  } else {
    ESP_LOGW(TAG, "Topic '%.*s' not found", topic_name.size(),
             topic_name.data());
  }

  xSemaphoreGiveRecursive(mutex_);
  return result;
}

std::optional<std::string_view> Bus::get_topic_name(TopicHandle topic) const {
  if (xSemaphoreTakeRecursive(mutex_, pdMS_TO_TICKS(100)) != pdTRUE) {
    return std::nullopt;
  }

  int index = find_topic_index_by_id(topic);
  std::optional<std::string_view> result = std::nullopt;

  if (index >= 0) {
    result = topics_[index].get_name();
  }

  xSemaphoreGiveRecursive(mutex_);
  return result;
}

SubscriberHandle Bus::subscribe(TopicHandle topic, SubscriberCallback callback,
                                void *context) {
  if (topic == INVALID_TOPIC) {
    ESP_LOGE(TAG, "Invalid topic");
    return SubscriberHandle{};
  }

  if (!callback) {
    ESP_LOGE(TAG, "Callback cannot be null");
    return SubscriberHandle{};
  }

  if (xSemaphoreTakeRecursive(mutex_, pdMS_TO_TICKS(100)) != pdTRUE) {
    ESP_LOGE(TAG, "Failed to acquire mutex");
    return SubscriberHandle{};
  }

  int topic_idx = find_topic_index_by_id(topic);
  if (topic_idx < 0) {
    xSemaphoreGiveRecursive(mutex_);
    ESP_LOGE(TAG, "Topic ID %lu not found", topic);
    return SubscriberHandle{};
  }

  Subscriber *sub = nullptr;
  auto &topic_subs = subscribers_[topic_idx];

  auto it = std::find_if(topic_subs.begin(), topic_subs.end(),
                         [](const Subscriber &s) { return !s.active; });

  if (it != topic_subs.end()) {
    sub = &(*it);
    sub->topic = topic;
    sub->callback = std::move(callback);
    sub->context = context;
    sub->active = true;
  }

  xSemaphoreGiveRecursive(mutex_);

  if (sub == nullptr) {
    ESP_LOGE(TAG, "No available subscriber slots for topic '%.*s'",
             static_cast<int>(topics_[topic_idx].get_name().length()),
             topics_[topic_idx].get_name().data());
    return SubscriberHandle{};
  }

  ESP_LOGI(TAG, "Subscriber added to topic: '%.*s'",
           static_cast<int>(topics_[topic_idx].get_name().length()),
           topics_[topic_idx].get_name().data());
  return SubscriberHandle{sub};
}

esp_err_t Bus::unsubscribe(SubscriberHandle handle) {
  if (!handle.is_valid()) {
    return ESP_ERR_INVALID_ARG;
  }

  auto *sub = static_cast<Subscriber *>(handle.get());

  if (xSemaphoreTakeRecursive(mutex_, pdMS_TO_TICKS(100)) != pdTRUE) {
    ESP_LOGE(TAG, "Failed to acquire mutex");
    return ESP_ERR_TIMEOUT;
  }

  sub->active = false;
  sub->callback = nullptr;
  sub->context = nullptr;

  xSemaphoreGiveRecursive(mutex_);

  ESP_LOGI(TAG, "Subscriber removed");
  return ESP_OK;
}

esp_err_t Bus::publish(const Event &event) {
  if (!running_ || event_queue_ == nullptr) {
    ESP_LOGE(TAG, "Event bus not initialized");
    return ESP_ERR_INVALID_STATE;
  }

  if (event.topic == INVALID_TOPIC) {
    ESP_LOGE(TAG, "Invalid event");
    return ESP_ERR_INVALID_ARG;
  }

  std::string_view topic_name = get_topic_name(event.topic).value_or("");

  ESP_LOGI(TAG, "Publishing event on topic '%.*s' (ID: %lu)", topic_name.size(),
           topic_name.data(), event.topic);

  InternalEvent internal_event{};
  internal_event.event = event;

  if (event.payload != nullptr && event.payload_size > 0) {
    size_t copy_size = std::min(event.payload_size, PAYLOAD_BUFFER_SIZE);
    if (event.payload_size > PAYLOAD_BUFFER_SIZE) {
      ESP_LOGW(TAG, "Payload too large (%zu bytes), truncating to %zu",
               event.payload_size, PAYLOAD_BUFFER_SIZE);
    }
    std::memcpy(internal_event.payload_buffer.data(), event.payload, copy_size);
    internal_event.event.payload = internal_event.payload_buffer.data();
    internal_event.event.payload_size = copy_size;
  }

  if (xQueueSend(event_queue_, &internal_event, pdMS_TO_TICKS(10)) != pdTRUE) {
    ESP_LOGW(TAG, "Event queue full, event dropped");
    return ESP_ERR_TIMEOUT;
  }

  return ESP_OK;
}

esp_err_t Bus::publish_from_isr(const Event &event) {
  if (!running_ || event_queue_ == nullptr) {
    return ESP_ERR_INVALID_STATE;
  }

  if (event.topic == INVALID_TOPIC) {
    return ESP_ERR_INVALID_ARG;
  }

  InternalEvent internal_event{};
  internal_event.event = event;

  if (event.payload != nullptr && event.payload_size > 0) {
    size_t copy_size = std::min(event.payload_size, PAYLOAD_BUFFER_SIZE);
    std::memcpy(internal_event.payload_buffer.data(), event.payload, copy_size);
    internal_event.event.payload = internal_event.payload_buffer.data();
    internal_event.event.payload_size = copy_size;
  }

  BaseType_t xHigherPriorityTaskWoken = pdFALSE;

  if (xQueueSendFromISR(event_queue_, &internal_event,
                        &xHigherPriorityTaskWoken) != pdTRUE) {
    return ESP_ERR_TIMEOUT;
  }

  portYIELD_FROM_ISR(xHigherPriorityTaskWoken);
  return ESP_OK;
}

void Bus::event_task(void *pvParameters) {
  auto *bus = static_cast<Bus *>(pvParameters);
  bus->process_events();
}

void Bus::process_events() {
  InternalEvent internal_event;

  ESP_LOGI(TAG, "Event bus task started");

  while (running_) {
    if (xQueueReceive(event_queue_, &internal_event, pdMS_TO_TICKS(100)) ==
        pdTRUE) {
      if (internal_event.event.payload_size > 0) {
        internal_event.event.payload = internal_event.payload_buffer.data();
      }

      const Event &event = internal_event.event;

      auto topic_name = get_topic_name(event.topic);
      ESP_LOGD(TAG, "Processing event on topic: %.*s",
               topic_name ? static_cast<int>(topic_name->length()) : 7,
               topic_name ? topic_name->data() : "UNKNOWN");

      if (xSemaphoreTakeRecursive(mutex_, pdMS_TO_TICKS(100)) == pdTRUE) {
        int topic_idx = find_topic_index_by_id(event.topic);

        if (topic_idx >= 0) {
          // Notify all subscribers
          for (auto &sub : subscribers_[topic_idx]) {
            if (sub.active && sub.callback) {
              sub.callback(event, sub.context);
            }
          }
        }

        xSemaphoreGiveRecursive(mutex_);
      }
    }
  }

  ESP_LOGI(TAG, "Event bus task stopping");
  vTaskDelete(nullptr);
}

void Bus::stop() {
  ESP_LOGI(TAG, "Stopping event bus");
  running_ = false;

  vTaskDelay(pdMS_TO_TICKS(200));

  if (event_queue_ != nullptr) {
    vQueueDelete(event_queue_);
    event_queue_ = nullptr;
  }

  if (mutex_ != nullptr) {
    vSemaphoreDelete(mutex_);
    mutex_ = nullptr;
  }

  topics_.fill(TopicEntry{});
  for (auto &topic_subs : subscribers_) {
    topic_subs.fill(Subscriber{});
  }
  next_topic_id_ = 0;
}

} // namespace EventBus
