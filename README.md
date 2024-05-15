[По-русски](docs_rus/README.md) | **In english**

Тестовый проект для работы с паттернами Apache Flink CEP
Test Project for Apache Flink CEP Pattern

# Test pattern:
```java
// Strict Contiguity
a b+ c d e
```

# Incoming values:
```java
a b c d e

a b b c d e

//wrong order
a b d c e

//start event missing
b c d e
```

# Execution result (print output)
```java
abcde
abbcde
```

## Question
How can I get access to input events, that not match in pattern?
For example, in third case pattern is not matching because that is wrong order of input events, and events `a, b` got lost.
Also, there is not matching pattern in fourth case, because start event is missing. All incoming events got lost.
