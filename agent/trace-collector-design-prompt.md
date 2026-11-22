# Role
You are an expert Java, Spring Boot, Tempo, Grafana, Opentelemetry consultant. 

# Goal
Your goal is to design a system for collecting, storing, enriching traces, produced by microservices that use the starter
otlp-kafka-export-tracing-starter. 

# Details
Microservices will send traces to a kafka topic in the format of a TraceMessage. 

The system must provide a mechanism for enriching the traces, collected from the topic. 

Enrichment sources are kafka topics as well, and they are not known beforehand. That is, the system must provide
a mechanism for a client, using it, to describe the enrichment source, how to connect to it, how to extract fields,
that will be used for enrichment, etc. It is assumed that all events, coming from those enrichment topics will contain
trace headers, so that it is possible to enrich traces. E.g. the system received a number of trace events from main trace topic,
then it receives an event from an enrichment topic, the headers contain trace_id information, and description of the 
enrichment source shows, which fields must be used to enrich the traces for that trace_id. 

The system must provide a mechanism to link traces together. Let's say, we use the system in an Identity Provider microservices.
A user is authenticated with the IdP by first factor - this is one trace_id, then the second factor - this is second trace_id,
then authorization code is exchanged for access, refresh and id tokens - this is third trace_id, but all of them are related
to a single business operation, that can be correlated by something like a business operation id. Therefore this system must
provide a mechanism for linking traces together with this business operation id. 

The system must provide mechanism for querying the traces by trace_id, by a date range, by trace attributes, that are added 
to the traces by microservices using the system. E.g. let's again take the Identity Provider. Microservices will add trace
attributes like user_id - main identifier of a user being authenticated with the IdP. client_id - main identifier of a client 
the user is authenticated to - I mean the user with user_id wants to be authenticated by means of this IdP in a web application, say, 
some book store. Here the book store is the client of the IdP with unique client_id. Among other attributes provided by the IdP
microservices may be authentication factors like (SMS, PUSH, COOKIE, QR etc.). But this is only an example, it is assumed that
the tracing system could be used by any microservices applications written in Java and Spring Boot 4+. 

In terms of efficiency, the system must be able to handle a sampling probability of 1.0, that is all the requests are traced. 
You must calculate the storage requirements in terms of RAM, CPU, disk space etc, as well as the type of the storage. 
The goal is to use PostgreSQL as the primary storage, but other types may also be considered as alternatives (only as alternatives). 

The queried results must be shown on a custom UI written in React.

You must provide both high level C4 design of the system, as well as a lower level design of the microservices in that 
trace collecting system. The microservices must be written in Java 25, Spring Boot 4.1.2 or higher. 

# Restrictions
No kafka streams are acceptable, because they must create topics on demand dynamically - it is not permitted in many
systems. If you CAN though provide a system that uses kafka streams with all the necessary topics, created beforehand,
then it is a viable option as well. 

Your design must take all the best properties of currently existing trace collection services like Tempo or Opentelemetry or
others. You MUST first examine their architecture before proceeding. 

You do not assume anything. If in doubt - go see source code or official documentation. 

# Expected output
You MUST provide output in the form of several markdown files in English in this directory (agent). 
They must contain: 
- overview of current technologies like Tempo, Opentelemetry etc.
- high level overview of the system that is being designed
- detailed low level overview of the system that is being designed
- detailed low level overview of the storage system to be used
- detailed low level overview of the domain model to be used in the system
- detailed low level overview of the use cases that the system will handle 
- sequence diagrams
- ER diagrams

Since the task is VERY complex and expensive in terms of tokens and context used by the model, use several independent subagents
to explore it. 