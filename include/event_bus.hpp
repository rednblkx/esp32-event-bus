#ifndef EVENT_BUS_HPP
#define EVENT_BUS_HPP

#include <algorithm>
#include <array>
#include <cstdint>
#include <functional>
#include <optional>
#include <string_view>

#include "freertos/FreeRTOS.h"
#include "freertos/queue.h"
#include "freertos/semphr.h"
#include "freertos/task.h"

namespace EventBus {

inline constexpr size_t MAX_TOPICS = 32;
inline constexpr size_t MAX_SUBSCRIBERS_PER_TOPIC = 8;
inline constexpr size_t MAX_TOPIC_NAME_LEN = 64;
inline constexpr size_t PAYLOAD_BUFFER_SIZE = 128;

using TopicHandle = uint32_t;
inline constexpr TopicHandle INVALID_TOPIC = 0xFFFFFFFF;

struct Event {
  TopicHandle topic{INVALID_TOPIC};
  uint32_t data{0};
  const void *payload{nullptr};
  size_t payload_size{0};

  Event() = default;
  Event(TopicHandle t, uint32_t d = 0, const void *p = nullptr, size_t ps = 0)
      : topic(t), data(d), payload(p), payload_size(ps) {}
};

using SubscriberCallback = std::function<void(const Event &, void *)>;

class SubscriberHandle {
public:
  SubscriberHandle() = default;
  explicit SubscriberHandle(void *ptr) : ptr_(ptr) {}

  bool is_valid() const { return ptr_ != nullptr; }
  void *get() const { return ptr_; }

private:
  void *ptr_{nullptr};
};

class Bus {
public:
  static Bus &instance();

  Bus(const Bus &) = delete;
  Bus &operator=(const Bus &) = delete;
  Bus(Bus &&) = delete;
  Bus &operator=(Bus &&) = delete;

  esp_err_t init();

  [[nodiscard]] TopicHandle register_topic(std::string_view topic_name,
                                           bool cache_last_message = false);

  [[nodiscard]] std::optional<TopicHandle>
  get_topic(std::string_view topic_name) const;

  [[nodiscard]] std::optional<std::string_view>
  get_topic_name(TopicHandle topic) const;

  [[nodiscard]] SubscriberHandle subscribe(TopicHandle topic,
                                           SubscriberCallback callback,
                                           void *context = nullptr);

  esp_err_t unsubscribe(SubscriberHandle handle);

  esp_err_t publish(const Event &event);

  esp_err_t publish_from_isr(const Event &event);

  void stop();

  [[nodiscard]] bool is_running() const { return running_; }

private:
  Bus();
  ~Bus();

  struct InternalEvent {
    Event event{};
    std::array<uint8_t, PAYLOAD_BUFFER_SIZE> payload_buffer{};
  };

  struct TopicEntry {
    std::array<char, MAX_TOPIC_NAME_LEN> name{};
    TopicHandle id{INVALID_TOPIC};
    bool active{false};
    bool cache_last_message{false};
    std::optional<InternalEvent> cached_event{std::nullopt};

    void set_name(std::string_view n) {
      size_t len = std::min(n.length(), MAX_TOPIC_NAME_LEN - 1);
      std::copy_n(n.begin(), len, name.begin());
      name[len] = '\0';
    }

    std::string_view get_name() const { return std::string_view(name.data()); }
  };

  struct Subscriber {
    TopicHandle topic{INVALID_TOPIC};
    SubscriberCallback callback{nullptr};
    void *context{nullptr};
    bool active{false};
  };

  static void event_task(void *pvParameters);
  void process_events();

  [[nodiscard]] int find_topic_index_by_id(TopicHandle topic_id) const;
  [[nodiscard]] int find_topic_index_by_name(std::string_view name) const;

  QueueHandle_t event_queue_{nullptr};
  TaskHandle_t event_task_handle_{nullptr};
  SemaphoreHandle_t mutex_{nullptr};

  std::array<TopicEntry, MAX_TOPICS> topics_{};
  std::array<std::array<Subscriber, MAX_SUBSCRIBERS_PER_TOPIC>, MAX_TOPICS>
      subscribers_{};

  uint32_t next_topic_id_{0};
  bool running_{false};

  static constexpr const char *TAG = "EventBus";
  static constexpr size_t EVENT_QUEUE_SIZE = 32;
  static constexpr size_t EVENT_TASK_STACK_SIZE = 4096;
  static constexpr UBaseType_t EVENT_TASK_PRIORITY = 5;
};

} // namespace EventBus

#endif // EVENT_BUS_HPP
