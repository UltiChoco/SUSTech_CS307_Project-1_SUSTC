package org.example.Task3;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class JsonParamReader {
    private final Map<String, Object> params = new HashMap<>();

    public JsonParamReader(String fileName) {
        loadParams(fileName);
    }

    private void loadParams(String fileName) {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(fileName)) {
            if (inputStream == null) {
                throw new IllegalArgumentException("配置文件不存在：" + fileName);
            }

            ObjectMapper objectMapper = new ObjectMapper();
            params.putAll(objectMapper.readValue(inputStream, new TypeReference<Map<String, Object>>() {}));

        } catch (Exception e) {
            throw new RuntimeException("加载JSON配置文件失败：" + fileName, e);
        }
    }

    public Map<String, Object> getAllParams() {
        return new HashMap<>(params);
    }

    public Optional<Object> getParam(String key) {
        return Optional.ofNullable(params.get(key));
    }

    public Optional<String> getString(String key) {
        return getParam(key).map(Object::toString);
    }

    public Optional<Integer> getInt(String key) {
        return getParam(key).map(value -> {
            if (value instanceof Number) {
                return ((Number) value).intValue();
            } else if (value instanceof String) {
                return Integer.parseInt((String) value);
            }
            return null;
        });
    }

    public Optional<Boolean> getBoolean(String key) {
        return getParam(key).map(value -> {
            if (value instanceof Boolean) {
                return (Boolean) value;
            } else if (value instanceof String) {
                return Boolean.parseBoolean((String) value);
            }
            return null;
        });
    }

    public Optional<Long> getLong(String key) {
        return getParam(key).map(value -> {
            if (value instanceof Number) {
                return ((Number) value).longValue();
            } else if (value instanceof String) {
                return Long.parseLong((String) value);
            }
            return null;
        });
    }
}
