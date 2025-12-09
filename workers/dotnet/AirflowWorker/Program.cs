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

var workerIdOption = new Option<string>("--worker-id", "Unique worker identifier") { Arity = ArgumentArity.ExactlyOne };
var taskQueueOption = new Option<string>("--task-queue-url", "SQS task queue URL") { Arity = ArgumentArity.ExactlyOne };
var resultQueueOption = new Option<string>("--result-queue-url", "SQS result queue URL") { Arity = ArgumentArity.ExactlyOne };
var idleTimeoutOption = new Option<int>("--idle-timeout", "Idle timeout in seconds");
idleTimeoutOption.DefaultValueFactory = _ => 300;
var visibilityTimeoutOption = new Option<int>("--visibility-timeout", "SQS visibility timeout");
visibilityTimeoutOption.DefaultValueFactory = _ => 3600;
var maxTasksOption = new Option<int>("--max-tasks", "Max tasks before terminating (0=unlimited)");
maxTasksOption.DefaultValueFactory = _ => 0;
var initAssemblyOption = new Option<string?>("--init-assembly", "Assembly containing shared state initializer");
var initTypeOption = new Option<string>("--init-type", "Type name for initializer");
initTypeOption.DefaultValueFactory = _ => "SharedStateInitializer";

var rootCommand = new RootCommand("Airflow Batch Worker Pool - .NET Worker")
{
    workerIdOption,
    taskQueueOption,
    resultQueueOption,
    idleTimeoutOption,
    visibilityTimeoutOption,
    maxTasksOption,
    initAssemblyOption,
    initTypeOption
};

rootCommand.SetAction(async (parseResult, cancellationToken) =>
{
    var workerId = parseResult.GetValue(workerIdOption)!;
    var taskQueueUrl = parseResult.GetValue(taskQueueOption)!;
    var resultQueueUrl = parseResult.GetValue(resultQueueOption)!;
    var idleTimeout = parseResult.GetValue(idleTimeoutOption);
    var visibilityTimeout = parseResult.GetValue(visibilityTimeoutOption);
    var maxTasks = parseResult.GetValue(maxTasksOption);
    var initAssembly = parseResult.GetValue(initAssemblyOption);
    var initType = parseResult.GetValue(initTypeOption)!;

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

    await worker.RunAsync(cancellationToken);
});

return await rootCommand.Parse(args).InvokeAsync();
