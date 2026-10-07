// ESP32 físico + sensor DHT11/DHT22 -> MQTT público -> bridge -> Kafka
// Placa en Arduino IDE: "ESP32 Dev Module".
// Librerías (Library Manager): "DHT sensor library" (Adafruit), "Adafruit Unified Sensor",
// y "PubSubClient" (Nick O'Leary).
//
// Cableado (directo con cables Dupont, sin protoboard):
//   DHT VCC -> 3V3      DHT DATA -> GPIO 4      DHT GND -> GND

#include <WiFi.h>
#include <PubSubClient.h>
#include <DHT.h>
#include <time.h>

// ===================== CAMBIA ESTOS VALORES =====================
const char *WIFI_SSID = "TU_WIFI";
const char *WIFI_PASSWORD = "TU_CLAVE";
// test.mosquitto.org es PÚBLICO: usa un identificador propio y único.
// Debe ser el mismo que pongas en el comando del bridge.
const char *EQUIPO = "equipo01";
#define DHT_TYPE DHT11   // DHT11 (módulo de 3 pines); usa DHT22 si tu sensor es un DHT22
// ================================================================

#define DHT_PIN 4
const char *MQTT_HOST = "test.mosquitto.org";
const int MQTT_PORT = 1883;
const unsigned long INTERVALO_MS = 3000;   // una lectura cada 3 s

char MQTT_TOPIC[80];
char SENSOR_ID[64];

DHT dht(DHT_PIN, DHT_TYPE);
WiFiClient wifiClient;
PubSubClient mqttClient(wifiClient);
unsigned long ultimaLectura = 0;

void conectarWifi() {
  Serial.printf("Conectando a WiFi %s", WIFI_SSID);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }
  Serial.print("\nWiFi conectado, IP: ");
  Serial.println(WiFi.localIP());
}

void sincronizarReloj() {
  // El timestamp debe ser un epoch real: las ventanas de Spark usan el tiempo del evento.
  configTime(0, 0, "pool.ntp.org", "time.nist.gov");
  Serial.print("Sincronizando reloj por NTP");
  time_t ahora = time(nullptr);
  while (ahora < 1700000000) {
    delay(500);
    Serial.print(".");
    ahora = time(nullptr);
  }
  Serial.println(" listo");
}

void conectarMQTT() {
  while (!mqttClient.connected()) {
    wifiClient.stop();  // cierra el socket anterior antes de reintentar
    String clientId = "esp32-dht-" + String((uint32_t)esp_random(), HEX);
    Serial.println("Conectando a MQTT como " + clientId + " ...");
    if (mqttClient.connect(clientId.c_str())) {
      Serial.println("MQTT conectado a " + String(MQTT_HOST));
    } else {
      Serial.printf("Fallo MQTT, rc=%d. Reintentando en 5s\n", mqttClient.state());
      delay(5000);
    }
  }
}

void setup() {
  Serial.begin(115200);
  dht.begin();

  snprintf(MQTT_TOPIC, sizeof(MQTT_TOPIC), "lambda26/atmos-real/%s/lecturas", EQUIPO);
  snprintf(SENSOR_ID, sizeof(SENSOR_ID), "esp32-fisico-%s", EQUIPO);

  conectarWifi();
  sincronizarReloj();
  mqttClient.setServer(MQTT_HOST, MQTT_PORT);
  conectarMQTT();
  Serial.printf("Topic MQTT: %s\n", MQTT_TOPIC);
}

void loop() {
  if (WiFi.status() != WL_CONNECTED) conectarWifi();
  if (!mqttClient.connected()) conectarMQTT();
  mqttClient.loop();

  unsigned long ahora = millis();
  if (ahora - ultimaLectura < INTERVALO_MS) return;
  ultimaLectura = ahora;

  float temperatura = dht.readTemperature();
  float humedad = dht.readHumidity();
  if (isnan(temperatura) || isnan(humedad)) {
    Serial.println("Error leyendo el DHT (revisa cableado y DHT_TYPE), se omite esta ronda");
    return;
  }

  // Sin sensor de presión físico no se inventa el campo: este topic lleva solo lo que se mide.
  // timestamp = segundos epoch + "000" -> milisegundos epoch (mismo formato que S7).
  char payload[220];
  snprintf(payload, sizeof(payload),
           "{\"tipoEvento\":\"sensor.lectura\",\"sensorId\":\"%s\",\"temperatura\":%.1f,"
           "\"humedad\":%.1f,\"origen\":\"esp32-fisico\",\"timestamp\":%ld000}",
           SENSOR_ID, temperatura, humedad, (long)time(nullptr));

  if (mqttClient.publish(MQTT_TOPIC, payload)) {
    Serial.print("Publicado: ");
    Serial.println(payload);
  } else {
    Serial.println("Fallo al publicar en MQTT");
  }
}
