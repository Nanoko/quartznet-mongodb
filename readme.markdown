Quartz.NET-MongoDB
======================================================================

## Overview
[Quartz.NET](http://quartznet.sourceforge.net/) Quartz.NET is a full-featured, open source job scheduling system that can be used from smallest apps to large scale enterprise systems.

This provider enables [MongoDB](http://www.mongodb.org/) to be used as the back-end job store via the [Official 10gen provider](http://www.mongodb.org/display/DOCS/CSharp+Language+Center).
Hugely inspired by the [Elmah-MongoDB project](https://github.com/CaptainCodeman/elmah-mongodb). It is an adaptation of the original "RAMJobStore" with custom BSON serializers.

## Usage (Please wait a few days...)
The easiest way to add this to a project is via the [quartz.mongodb NuGET package](http://nuget.org/List/Packages/quartz.mongodb) which will add the required assemblies to your project.

## Configuration
Here is an example configuration:

    <quartz>
      <add key="quartz.jobStore.type" value="Quartz.Impl.MongoDB.JobStore, Quartz.Impl.MongoDB"/>
    </quartz>
    <connectionStrings>
      <add name="quartznet-mongodb" connectionString="server=localhost;database=quartznet;"/>
    </connectionStrings>

