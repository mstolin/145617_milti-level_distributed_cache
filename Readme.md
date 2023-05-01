# Multi-Level Distributed Cache

## Description

This is the implementation of a multi-level distributed cache for the course
Distributed Systems 1 at University of Trento. A presentation and description
can be found at 
[DS1_project_2022_presentation.pdf](./DS1_project_2022_presentation.pdf) and
[DS1_project_2022_description.pdf](./DS1_project_2022_description.pdf). A more 
detailed report about the design choice and implementation is available at
[report.pdf](./report.pdf).

## Usage

This project was developed using OpenJDK 18.0.2 
(https://openjdk.org/projects/jdk/18/) and Gradle 7.6 (https://gradle.org/).

The preferred way to install the dependencies is by using 
[SDKMAN!](https://sdkman.io/). See the provided [.sdkmanrc](./.sdkmanrc).

The `Main.java` contains multiple scenarios. You can start the main by running
`$ gradle run`.