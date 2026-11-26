# Configuration and certificates for MQTT tests

- `ca.crt`, `ca.key`: CA certificate and private key
- `client.crt`, `client.key`: Client certificate and private key signed by CA
- `mosquitto.conf`: MQTT broker configuration
- `README.md`: this file
- `server.crt`, `server.key`: Server certificate and private key signed by CA with CN=mqtt
- `unsigned.crt`, `unsigned.key`: Client certificate and private key *not* signed by CA

## Re-issuing `server.crt` (keeps CA, key, and subject; SAN is REQUIRED)

The server cert must carry `subjectAltName = DNS:mqtt, DNS:localhost, IP:127.0.0.1` — in-network
clients verify the `mqtt` container-DNS name, host-run clients (Ditto on the host, `local-*` envs)
verify `localhost`. A re-issue without the SAN silently re-breaks the host-run
`testClientCertificateAuthentication` tests (JVM hostname verification). Recipe used 2026-07-16
(reconstructed from the original cert's parameters; no recipe existed before):

```sh
cd docker/mqtt
openssl req -new -key server.key \
  -subj "/C=DE/ST=BW/O=Test Server/CN=mqtt" -out server.csr
printf 'subjectAltName = DNS:mqtt, DNS:localhost, IP:127.0.0.1\n' > san.ext
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -extfile san.ext -days 26831 -sha256 -out server.crt
rm server.csr san.ext
# verify:
openssl x509 -in server.crt -text -noout | grep -A1 'Subject Alternative Name'
```

Then `docker restart docker-mqtt-1` (or the compose `mqtt` service) to serve the new cert.
