/**
 * @file main.c
 * @brief ESP32-CAM + VEML7700 — capture JPEG & lux, send via HTTPS/MQTT
 *
 * Tasks:
 *   sensor_camera_task — 15 s period, lux → MQTT JSON, JPEG → HTTPS POST
 *
 * Init order (important):
 *   1. VEML7700  (installs new I2C driver on I2C_NUM_1)
 *   2. Camera    (uses its own SCCB on I2C_NUM_0, GPIO 26/27)
 *   3. Wi-Fi     (BLE provisioning via wifi_prov_init, blocks until IP)
 *   4. OTA       (camera deinit → flash write → camera reinit, avoids DMA conflict)
 *   5. SNTP      (waits for time sync via TIME_SYNCED_BIT, callback-driven)
 *   6. MQTT      (waits for CONNACK via MQTT_CONNECTED_BIT)
 */
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/semphr.h"
#include "freertos/event_groups.h"

#include "esp_log.h"
#include "esp_mac.h"
#include "esp_system.h"
#include "esp_event.h"
#include "esp_sntp.h"
#include "esp_ota_ops.h"
#include "nvs_flash.h"

#include "esp_http_client.h"
#include "esp_crt_bundle.h"
#include "mqtt_client.h"
#include "esp_camera.h"

#include "camera_drv.h"
#include "veml7700_drv.h"
#include "wifi_prov/wifi_prov.h"
#include "ota/ota.h"

/* ═══════════════════════════════════════════════════════════
 *  Configuration — edit these for your environment
 * ═══════════════════════════════════════════════════════════ */

#define HTTP_POST_URL          "http://ec2-100-54-40-70.compute-1.amazonaws.com:8282"
#define MQTT_BROKER_URI        "mqtt://ec2-100-54-40-70.compute-1.amazonaws.com:1883"
#define MQTT_TOPIC_BASE        "onedegree/sensor/veml7700/device"
#define MQTT_LWT_MSG           "{\"status\":\"offline\"}"
#define MQTT_ONLINE_MSG        "{\"status\":\"online\"}"

#define TASK_PERIOD_MS         15000   /* 15초 */

#define SNTP_SYNC_TIMEOUT_MS   30000   /* 30 s — SNTP 동기화 대기 */

/* Sanity threshold for time sync: any time before 2023-11-14 is treated as
 * "not yet synced" (prevents 1970 timestamps from leaking into payloads). */
#define MIN_VALID_EPOCH        1700000000

/* ═══════════════════════════════════════════════════════════ */

static const char *TAG = "MAIN";

/* ── Event group bits ── */
#define MQTT_CONNECTED_BIT     BIT1
#define TIME_SYNCED_BIT        BIT2
static EventGroupHandle_t s_app_events;

/* Shared I2C mutex */
static SemaphoreHandle_t s_i2c_mutex;

/* MQTT client handle */
static esp_mqtt_client_handle_t s_mqtt_client = NULL;
static volatile bool s_mqtt_connected = false;

/* Sensor identifier: Wi-Fi STA MAC address (last 3 bytes, hex) */
static char s_sensor_no[18];
static char s_topic_lux[72];
static char s_topic_status[72];

/* ────────────────── Sensor ID ───────────────────────────── */

static void sensor_no_init(void)
{
    uint8_t mac[6];
    ESP_ERROR_CHECK(esp_read_mac(mac, ESP_MAC_WIFI_STA));
    snprintf(s_sensor_no, sizeof(s_sensor_no), "%02X%02X%02X", mac[3], mac[4], mac[5]);
    snprintf(s_topic_lux,    sizeof(s_topic_lux),
             MQTT_TOPIC_BASE "/%s/lux",    s_sensor_no);
    snprintf(s_topic_status, sizeof(s_topic_status),
             MQTT_TOPIC_BASE "/%s/status", s_sensor_no);
    ESP_LOGI(TAG, "Sensor ID (MAC): %s", s_sensor_no);
    ESP_LOGI(TAG, "MQTT topics: %s | %s", s_topic_lux, s_topic_status);
}

/* ────────────────── SNTP (for timestamps) ────────────────── */

static void time_sync_notification_cb(struct timeval *tv)
{
    ESP_LOGI(TAG, "SNTP sync notification (epoch=%lld)", (long long)tv->tv_sec);
    xEventGroupSetBits(s_app_events, TIME_SYNCED_BIT);
}

static void sntp_init_time(void)
{
    setenv("TZ", "KST-9", 1);
    tzset();

    ESP_LOGI(TAG, "Initialising SNTP …");
    esp_sntp_setoperatingmode(SNTP_OPMODE_POLL);
    esp_sntp_setservername(0, "pool.ntp.org");
    sntp_set_time_sync_notification_cb(time_sync_notification_cb);
    esp_sntp_init();

    EventBits_t bits = xEventGroupWaitBits(
        s_app_events, TIME_SYNCED_BIT,
        pdFALSE, pdTRUE, pdMS_TO_TICKS(SNTP_SYNC_TIMEOUT_MS));

    if (bits & TIME_SYNCED_BIT) {
        ESP_LOGI(TAG, "SNTP synchronised");
    } else {
        ESP_LOGW(TAG, "SNTP sync timeout — task will retry-guard via runtime check");
    }
}

/* ────────────────── MQTT ─────────────────────────────────── */

static void mqtt_event_handler(void *arg, esp_event_base_t base,
                               int32_t id, void *data)
{
    esp_mqtt_event_handle_t evt = (esp_mqtt_event_handle_t)data;

    switch ((esp_mqtt_event_id_t)id) {
    case MQTT_EVENT_CONNECTED:
        ESP_LOGI(TAG, "MQTT connected");
        s_mqtt_connected = true;
        xEventGroupSetBits(s_app_events, MQTT_CONNECTED_BIT);
        esp_mqtt_client_publish(s_mqtt_client, s_topic_status,
                                MQTT_ONLINE_MSG, 0, 1, 1);
        break;
    case MQTT_EVENT_DISCONNECTED:
        ESP_LOGW(TAG, "MQTT disconnected");
        s_mqtt_connected = false;
        xEventGroupClearBits(s_app_events, MQTT_CONNECTED_BIT);
        break;
    case MQTT_EVENT_ERROR:
        ESP_LOGE(TAG, "MQTT error type=%d", evt->error_handle->error_type);
        break;
    default:
        break;
    }
}

static void mqtt_init(void)
{
    const esp_mqtt_client_config_t cfg = {
        .broker.address.uri = MQTT_BROKER_URI,
        .session = {
            .last_will = {
                .topic   = s_topic_status,
                .msg     = MQTT_LWT_MSG,
                .msg_len = strlen(MQTT_LWT_MSG),
                .qos     = 1,
                .retain  = 1,
            },
            .keepalive = 30,
        },
        .network.reconnect_timeout_ms = 5000,
    };

    s_mqtt_client = esp_mqtt_client_init(&cfg);
    esp_mqtt_client_register_event(s_mqtt_client, ESP_EVENT_ANY_ID,
                                   mqtt_event_handler, NULL);
    esp_mqtt_client_start(s_mqtt_client);
    ESP_LOGI(TAG, "MQTT client started → %s", MQTT_BROKER_URI);
}

static void wait_for_mqtt(void)
{
    ESP_LOGI(TAG, "Waiting for MQTT to connect...");
    xEventGroupWaitBits(s_app_events, MQTT_CONNECTED_BIT,
                        pdFALSE, pdTRUE, portMAX_DELAY);
    ESP_LOGI(TAG, "MQTT connected ✓");
}

/* ────────────────── Timestamp helper ─────────────────────── */

static void get_kst_timestamp(char *buf, size_t len)
{
    time_t now;
    struct tm ti;
    time(&now);
    localtime_r(&now, &ti);
    strftime(buf, len, "%Y-%m-%dT%H:%M:%S+09:00", &ti);
}

static bool is_time_valid(void)
{
    time_t now;
    time(&now);
    return now >= MIN_VALID_EPOCH;
}

/* ────────────────── Unified sensor + camera task ────────── */

static void sensor_camera_task(void *arg)
{
    char ts[40];
    char payload[128];
    bool first_run = true;

    while (1) {

        if (!first_run) {
            vTaskDelay(pdMS_TO_TICKS(TASK_PERIOD_MS));
        }
        first_run = false;

        if (!is_time_valid()) {
            ESP_LOGW(TAG, "Time not yet synced — skipping this cycle");
            continue;
        }

        get_kst_timestamp(ts, sizeof(ts));

        /* ── 조도 측정 ── */
        float lux = 0.0f;
        esp_err_t ret = veml7700_read_lux(&lux);
        if (ret == ESP_OK) {
            snprintf(payload, sizeof(payload),
                     "{\"lux\":%.2f,\"timestamp\":\"%s\"}", lux, ts);
            ESP_LOGI(TAG, "Lux=%.2f  ts=%s", lux, ts);
            if (s_mqtt_connected) {
                int msg_id = esp_mqtt_client_publish(
                    s_mqtt_client, s_topic_lux,
                    payload, 0, /*qos*/1, /*retain*/0);
                if (msg_id < 0) {
                    ESP_LOGW(TAG, "MQTT publish failed");
                }
            } else {
                ESP_LOGW(TAG, "MQTT not connected, skipping publish");
            }
        } else {
            ESP_LOGW(TAG, "Lux read failed: %s", esp_err_to_name(ret));
        }

        /* ── 카메라 촬영 ── */
        if (xSemaphoreTake(s_i2c_mutex, pdMS_TO_TICKS(2000)) != pdTRUE) {
            ESP_LOGW(TAG, "camera: mutex timeout");
            continue;
        }

        camera_fb_t *fb = esp_camera_fb_get();
        xSemaphoreGive(s_i2c_mutex);

        if (!fb) {
            ESP_LOGE(TAG, "Camera capture failed");
            continue;
        }

        ESP_LOGI(TAG, "JPEG captured: %u bytes  ts=%s", (unsigned)fb->len, ts);

        /* HTTP POST */
        esp_http_client_config_t http_cfg = {
            .url               = HTTP_POST_URL,
            .method            = HTTP_METHOD_POST,
            .timeout_ms        = 15000,
            .crt_bundle_attach = NULL,
            .skip_cert_common_name_check = true,
            .use_global_ca_store = false,
        };
        esp_http_client_handle_t client = esp_http_client_init(&http_cfg);
        esp_http_client_set_header(client, "User-Agent",   "ESP32-CAM/1.0");
        esp_http_client_set_header(client, "Content-Type", "image/jpeg");
        esp_http_client_set_header(client, "X-Timestamp",  ts);
        esp_http_client_set_header(client, "X-Sensor-No",  s_sensor_no);
        esp_err_t err = esp_http_client_open(client, (int)fb->len);
        if (err == ESP_OK) {
            int written = esp_http_client_write(client, (const char *)fb->buf, (int)fb->len);
            if (written < 0) {
                ESP_LOGE(TAG, "HTTP write failed");
            } else {
                esp_http_client_fetch_headers(client);
                int status = esp_http_client_get_status_code(client);
                ESP_LOGI(TAG, "HTTP POST → %d", status);
            }
        } else {
            ESP_LOGE(TAG, "HTTP POST failed: %s", esp_err_to_name(err));
        }

        esp_http_client_cleanup(client);
        esp_camera_fb_return(fb);
    }
}

/* ────────────────── app_main ─────────────────────────────── */

void app_main(void)
{
    /* [0] OTA 롤백 방지 — 이 앱을 유효 파티션으로 확정 */
    esp_ota_mark_app_valid_cancel_rollback();

    /* NVS (required by Wi-Fi provisioning) */
    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES ||
        ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ret = nvs_flash_init();
    }
    ESP_ERROR_CHECK(ret);

    ESP_LOGI(TAG, "=== ESP32-CAM firmware v%s ===", CURRENT_FIRMWARE_VERSION);

    /* EventGroup (MQTT / TIME bits) */
    s_app_events = xEventGroupCreate();
    assert(s_app_events);

    /* Shared I2C mutex */
    s_i2c_mutex = xSemaphoreCreateMutex();
    assert(s_i2c_mutex);

    /* ─── 0. Read MAC address (sensor identifier) ─── */
    sensor_no_init();

    /* ─── 1. VEML7700 (installs I2C driver on NUM_1) ─── */
    ESP_ERROR_CHECK(veml7700_init(s_i2c_mutex));

    /* ─── 2. Camera ─── */
    ESP_ERROR_CHECK(camera_init());

    /* ─── 3. Wi-Fi — BLE provisioning (blocks until IP obtained) ─── */
    wifi_prov_init();

    /* ─── 4. OTA — deinit camera first to prevent DMA conflict during flash write ─── */
    esp_camera_deinit();
    ota_check_and_update();
    ESP_ERROR_CHECK(camera_init());

    /* ─── 5. SNTP (blocks until first sync or timeout) ─── */
    sntp_init_time();

    /* ─── 6. MQTT (blocks until CONNACK) ─── */
    mqtt_init();
    wait_for_mqtt();

    /* ─── 7. Launch unified task ─── */
    xTaskCreatePinnedToCore(sensor_camera_task, "sensor_camera_task",
                            8192, NULL, 5, NULL, 0);

    ESP_LOGI(TAG, "Task launched ✓");
}
