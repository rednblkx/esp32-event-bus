idf_component_register(SRCS src/event_bus.cpp
                      INCLUDE_DIRS include)
target_compile_definitions(${COMPONENT_LIB} PUBLIC "-DLOG_LOCAL_LEVEL=ESP_LOG_VERBOSE")