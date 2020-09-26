# Systems architecture

This document describes the system architecture for an open source version of Katikati.

A deployment includes a few basic components:

1. Nook (the browser bnased UI for having conversations)
2. A Google Cloud / Firebase project for hosting the data storage and communications infrastructure
3. A messaging adapter for connecting to a communications client such as RapidPro running in a Google Cloud Virtual Machine


These components are controlled by command line tools run from a developer's linux or Mac OS machine.
