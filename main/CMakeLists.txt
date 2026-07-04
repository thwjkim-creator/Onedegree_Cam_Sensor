idf_component_register(
    SRCS "main.c" "camera_drv.c" "veml7700_drv.c"
         "wifi_prov/wifi_prov.c"
         "ota/ota.c"
    INCLUDE_DIRS "." "wifi_prov" "ota"
    REQUIRES driver esp_wifi esp_event esp_http_client esp-tls mqtt nvs_flash esp_timer esp_psram mbedtls
             wifi_provisioning protocomm bt
             app_update esp_https_ota
)