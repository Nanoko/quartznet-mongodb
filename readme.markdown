Quartz.NET-MongoDB
======================================================================

## Overview
[Quartz.NET](http://quartznet.sourceforge.net/) Quartz.NET is a full-featured, open source job scheduling system that can be used from smallest apps to large scale enterprise systems.

This provider enables [MongoDB](http://www.mongodb.org/) to be used as the back-end job store via the [Official 10gen provider](http://www.mongodb.org/display/DOCS/CSharp+Language+Center).
Hugely inspired by the [Elmah-MongoDB project](https://github.com/CaptainCodeman/elmah-mongodb). It is an adaptation of the original "RAMJobStore" with custom BSON serializers.

## Usage
The easiest way to add this to a project is via the [quartz.impl.mongodb NuGET package](http://nuget.org/packages/quartz.impl.mongodb) which will add the required assemblies to your project.

## Configuration
Here is an example web or app config file snippet:

    <quartz>
      <add key="quartz.jobStore.type" value="Quartz.Impl.MongoDB.JobStore, Quartz.Impl.MongoDB"/>
    </quartz>
    <connectionStrings>
      <add name="quartznet-mongodb" connectionString="server=localhost;database=quartznet;"/>
    </connectionStrings>

You may want to use the in-code method to configure your scheduler as it is easier in a clustered environment.
Remember this will still need to set a valid connection string in web or app config file :

	// get a scheduler
	NameValueCollection properties = new NameValueCollection();
	properties["quartz.scheduler.instanceName"] = "MyApplicationScheduler"; // needed if you plan to use the same database for many schedulers
	properties["quartz.scheduler.instanceId"] = System.Environment.MachineName + DateTime.UtcNow.Ticks; // requires uniqueness
	properties["quartz.jobStore.type"] = "Quartz.Impl.MongoDB.JobStore, Quartz.Impl.MongoDB";
	
	IScheduler scheduler = new Quartz.Impl.StdSchedulerFactory(properties).GetScheduler();

