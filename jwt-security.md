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

## 4. JWT Flow Diagram

```mermaid
flowchart TD
    A[User Login to AKHQ] --> B[Basic Auth username and SHA256 password]
    B --> C[AKHQ validates credentials]
    C --> D[JWT Token Generated]
    D --> E[Client stores JWT]
    E --> F[Requests to AKHQ API]
    F --> G[JWT validated using secret key]
    G --> H[Access Kafka UI and Topics]

