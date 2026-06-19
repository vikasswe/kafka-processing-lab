# JWT in AKHQ

## 1. Create AKHQ configuration file

```bash
# Create configuration file
nano akhq.yml

# Generate SHA-256 password (copy and store safely)
echo -n "your_password_here" | sha256sum
```

## 2. AKHQ Configuration (akhq.yml)

```yml
micronaut:
  server:
    context-path: /kafka-server

  security:
    enabled: true
    authentication: bearer
    token:
      jwt:
        signatures:
          secret:
            generator:
              secret: "ALongSecretKeyThatIsAtLeast46CharactersInLengthForSecurity!!"

akhq:
  security:
    enabled: true
    default-group: no-roles

    basic-auth:
      - username: admin
        password: "240be518fabd2724ddb6f04eeb1da5967448d7e831c08c8fa822809f74c720a9"
        passwordHash: SHA256
        groups:
          - admin

  connections:
    my-kafka:
      properties:
        bootstrap.servers: "xxx.xxx.xx.xx:9092"
```

## 3. Notes / Important Points

- Always replace the generated SHA-256 password in the config before saving.
- Do not use default `JWT secret` in production.
- Ensure secret is long, random, and securely stored.
- Update `bootstrap.servers` according to your Kafka cluster.
- Avoid committing credentials to Git repositories.

**Need to run via this command:**

```bash
sudo docker run -d \
  --name akhq \
  -p 8080:8080 \
  -v $(pwd)/akhq.yml:/app/application.yml \
  -e MICRONAUT_CONFIG_FILES=/app/application.yml \
  tchiotludo/akhq:latest
```

## If you are using nginx configure this also

```bash
sudo nano /etc/nginx/sites-available/default
```

**Update this**:

```nginx
server {
    listen 8000 default_server;
    listen [::]:8000 default_server;

    server_name _;

    location / {
        proxy_pass http://localhost:4455;

        # Standard Forwarding Headers
        proxy_set_header Host $http_host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # ✅ CRITICAL FIX FOR BASIC AUTH (Preserved)
        proxy_set_header Authorization $http_authorization;
        proxy_pass_header Authorization;

        # ✅ WEBSOCKET SUPPORT FOR AKHQ LIVE TAILING
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";

        # ✅ TIMEOUT ADJUSTMENT FOR HIGH-VOLUME TOPIC BROWSING
        proxy_read_timeout 120s;
        proxy_send_timeout 120s;
    }
}
```

## JWT Flow Diagram

```mermaid
flowchart TD
    A[User Login to AKHQ] --> B[Basic Auth username and SHA256 password]
    B --> C[AKHQ validates credentials]
    C --> D[JWT Token Generated]
    D --> E[Client stores JWT]
    E --> F[Requests to AKHQ API]
    F --> G[JWT validated using secret key]
    G --> H[Access Kafka UI and Topics]
