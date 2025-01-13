#!/usr/bin/env bash
 
set -eu

PASSWORD="${PASSWORD:-supersecret}"

rm -rf certs/*
# Root CA
echo "Creating CA certificate and key"
openssl req -new -x509 -keyout certs/ca.key -out certs/ca.crt -days 365 -subj "/CN=Sample CA/OU=US/O=US/ST=US/C=US" -passout pass:$PASSWORD

echo "Creating Truststore"
keytool -keystore certs/kafka.truststore.jks -alias CARoot -import -file certs/ca.crt -storepass $PASSWORD -keypass $PASSWORD -noprompt

# Node certs

echo "Creating node key"
keytool -keystore certs/kafka.keystore.jks -alias kafka -validity 365 -genkey -keyalg RSA -dname "cn=kafka, ou=US, o=US, c=US" -storepass $PASSWORD -keypass $PASSWORD
echo "Creating certificate sign request"
keytool -keystore certs/kafka.keystore.jks -alias kafka -certreq -file certs/tls.srl -storepass $PASSWORD -keypass $PASSWORD
echo "Signing certificate request using self-signed CA"
openssl x509 -req -CA certs/ca.crt -CAkey certs/ca.key \
    -in certs/tls.srl -out certs/tls.crt \
    -days 365 -CAcreateserial \
    -passin pass:$PASSWORD
echo "Adding Ca certificate to the keystore"
keytool -keystore certs/kafka.keystore.jks -alias CARoot -import -file certs/ca.crt -storepass $PASSWORD -keypass $PASSWORD -noprompt
echo "Adding signed certificate"
keytool -keystore certs/kafka.keystore.jks -alias kafka -import -file certs/tls.crt -storepass $PASSWORD -keypass $PASSWORD -noprompt



# Client cert

echo "Creating client key"
keytool -keystore certs/kafka.keystore.jks -alias client -validity 365 -genkey -keyalg RSA -dname "cn=client, ou=US, o=US, c=US" -storepass $PASSWORD -keypass $PASSWORD

echo "Creating certificate sign request"
keytool -keystore certs/kafka.keystore.jks -alias client -certreq -file certs/client.srl -storepass $PASSWORD -keypass $PASSWORD


echo $PASSWORD | keytool -importkeystore \
    -srckeystore certs/kafka.keystore.jks \
    -destkeystore certs/kafka.keystore.p12 \
    -deststoretype PKCS12 \
    -srcalias client \
    -deststorepass $PASSWORD \
    -destkeypass $PASSWORD

openssl pkcs12 -in certs/kafka.keystore.p12 -nokeys -out certs/client_cert.pem -passin pass:$PASSWORD
openssl pkcs12 -in certs/kafka.keystore.p12 -nodes -nocerts -out certs/client_key.pem -passin pass:$PASSWORD

chmod 644 certs/client*
