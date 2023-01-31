# IntelliJ run configurations
For some IntelliJ run configurations a little background information is necessary.
The purpose of this readme is to provide this information.

## Prerequisites

The following Plugins are required:
* [Multirun](https://plugins.jetbrains.com/plugin/7248-multirun)

## System test environment
Unfortunately it's not possible to provide the run configuration for the system test environment automatically since it depends on the location where the system test repository is located.

This is how to create this run configuration manually:
* Open the _"Edit Configurations..."_ dialogue
* Click the "+" to add a new run configuration
* Select "docker-compose"
* Now select the following two compose files:
  * $testRepoRoot/docker/docker-compose.yml
  * $testRepoRoot/docker/docker-compose.override.yml
* In the "services" section you can define which services of the compose file you want to start. This is what I have: artemis, kafka, mqtt, oauth, rabbitmq, zookeeper, fluentbit, ssh, 

That's it. When you run this configuration, all external dependencies (except a mongo DB) which are required for running systemtests locally are getting started.

## Hints
* Having environment variable `REQUESTS_CA_BUNDLE` permanent may be helpful. I.e. add the export to your `bash_profile`
  for example.
* It might be necessary to replace your existing `ca-certificates.crt` with the one provided as download in the wiki.
* If starting the container via run configuration still fails, please try to restart IntelliJ.
