You are a AWS Cloud and Flink Expert. Below is a situation where you need to established intra communication between two flink application. 
Intention is No data loss between these two application. 
Intention is no downtime of the application.
Intention is throughput should be < 2 seconds

# Environment 
AWS cloud

#Source 
AWS MSK 
One single kafka topic with 24 partition 

# Processing Layer 
AWS managed flink 
Flink version 1.18
Flink KPU is configurable
State management as business data according to business rule is being stored here

# Sink Layer
DynamoDb

# Business Requirement 
a) No Downtime 
b) No message loss 
c) 2 seconds latency
d) Duplicate message processing is fine as DynamoDb is idempotent 


# Current Processing Layer 
Main Flink App (app1)
Standby Flink App (app2)
Current state of two app flow : 
App1 is running and building up the business data state(say stateX). Now if we need to upgrade app1, first we start app2(with a new start say stateSB) and let it start processing data 
properly. Then stop app1 and upgrade it. Then we start app1 and let it start it using stateX and processing data. Then we stop app2 and complete the process. 

# Problem 
Intra state mismatch between app1 and app2 and that would make the data in dynamodb wrong. 

# Solution 
I am open for a solution which can be hosted in EKS or ECS but the solution has to be using flink as it has high thoughtput capabilities. But there is a no open-source solution in the market where intra state communication happens between two flink apps. Lets design it and may be we can create a solution which can be brand new and we can open source it. But lets first concentrate on design it properly and what are our options ?