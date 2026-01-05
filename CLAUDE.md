Project Objectives: Aggregating Change Data Capture Events based on Transactional Boundaries 

System description:
Source Database: oracle 21c 10 Tables replicated
CDC: Debezium  Debezium Release Series 3.4

Output postgresql 17
Kafka clients 10 Topics
Using docker compose:

Demonstrate that implementing Oracle connector’s CTE query feature, https://debezium.io/blog/2025/08/14/oracle-new-feature-cte-query/ solves the issue of loosing Oracle transaction atomicity 
