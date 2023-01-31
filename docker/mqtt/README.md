# Configuration and certificates for MQTT tests

- `ca.crt`, `ca.key`: CA certificate and private key
- `client.crt`, `client.key`: Client certificate and private key signed by CA
- `mosquitto.conf`: MQTT broker configuration
- `README.md`: this file
- `server.crt`, `server.key`: Server certificate and private key signed by CA with CN=mqtt
- `unsigned.crt`, `unsigned.key`: Client certificate and private key *not* signed by CA
