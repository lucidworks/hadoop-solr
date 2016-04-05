# logstash re-packaging

The goal of this utility is to build the logstash package used in the GrokIngestMapper

## Overview

This set of tasks does the following:

* Downloads and unzips logstash
* Pushes all gems into the logstash-mapper jar
* Patches the environment.rb from logstash

## Requirements

You must have ruby installed to execute this utility. No other gems/extensions are required.

## Tasks

From the logstash-mapper folder, you can run `rake -T` to see a list of commands, but generally only `rake package` is needed.
`package` will download logstash, optimize a few things and produce a new `jar`.

## Uploading to Nexus
Once the `package` task has completed, you'll find a `work/logstash-mapper-with-gems.jar` file. This jar should be copied into `solr-hadoop-core/libs/`

Note: The jar can be generated as part of the build process, however, since its add complexity in our build (Ruby dependecy), the jar will be publish as part of the source code.