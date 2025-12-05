// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System.CommandLine;
using AirflowWorker;

var rootCommand = new RootCommand("Airflow Batch Worker Pool - .NET Worker");

var workerIdOption = new Option<string>("--worker-id", "Unique worker identifier") { IsRequired = true };
var taskQueueOption = new Option<string>("--task-queue-url", "SQS task queue URL") { IsRequired = true };
var resultQueueOption = new Option<string>("--result-queue-url", "SQS result queue URL") { IsRequired = true };
var idleTimeoutOption = new Option<int>("--idle-timeout", () => 300, "Idle timeout in seconds");
var visibilityTimeoutOption = new Option<int>("--visibility-timeout", () => 3600, "SQS visibility timeout");
var maxTasksOption = new Option<int>("--max-tasks", () => 0, "Max tasks before terminating (0=unlimited)");
var initAssemblyOption = new Option<string?>("--init-assembly", "Assembly containing shared state initializer");
var initTypeOption = new Option<string>("--init-type", () => "SharedStateInitializer", "Type name for initializer");

rootCommand.AddOption(workerIdOption);
rootCommand.AddOption(taskQueueOption);
rootCommand.AddOption(resultQueueOption);
rootCommand.AddOption(idleTimeoutOption);
rootCommand.AddOption(visibilityTimeoutOption);
rootCommand.AddOption(maxTasksOption);
rootCommand.AddOption(initAssemblyOption);
rootCommand.AddOption(initTypeOption);

rootCommand.SetHandler(async (context) =>
{
    var workerId = context.ParseResult.GetValueForOption(workerIdOption)!;
    var taskQueueUrl = context.ParseResult.GetValueForOption(taskQueueOption)!;
    var resultQueueUrl = context.ParseResult.GetValueForOption(resultQueueOption)!;
    var idleTimeout = context.ParseResult.GetValueForOption(idleTimeoutOption);
    var visibilityTimeout = context.ParseResult.GetValueForOption(visibilityTimeoutOption);
    var maxTasks = context.ParseResult.GetValueForOption(maxTasksOption);
    var initAssembly = context.ParseResult.GetValueForOption(initAssemblyOption);
    var initType = context.ParseResult.GetValueForOption(initTypeOption)!;

    var worker = new WorkerProcess(
        workerId,
        taskQueueUrl,
        resultQueueUrl,
        TimeSpan.FromSeconds(idleTimeout),
        TimeSpan.FromSeconds(visibilityTimeout),
        maxTasks,
        initAssembly,
        initType
    );

    await worker.RunAsync(context.GetCancellationToken());
});

return await rootCommand.InvokeAsync(args);
