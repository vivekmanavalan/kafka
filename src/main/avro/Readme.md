YOU (Schema Owner)                  TEAM B (Consumer)
──────────────────                  ──────────────────

order-event.avsc                    pom.xml:
  │                                   <dependency>
  │ mvn package                         order-events-schema:1.0.0
  │                                   </dependency>
  ▼                                        │
Generated:                               Gets:
  OrderEvent.java     ──── JAR ────▶    OrderEvent.class
  OrderStatus.java    ──── JAR ────▶    OrderStatus.class  
  OrderItem.java      ──── JAR ────▶    OrderItem.class
  
  │
  │ mvn schema-registry:register
  ▼
Schema Registry:
  order-events-value v1
  (binary schema stored)
  
At runtime:
  Producer serializes OrderEvent → [SchemaID=42 | binary bytes]
  Kafka stores those bytes
  Consumer gets bytes → asks registry "what is ID 42?" → deserializes → OrderEvent ✅