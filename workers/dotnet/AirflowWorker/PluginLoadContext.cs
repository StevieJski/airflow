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

using System.Reflection;
using System.Runtime.Loader;

namespace AirflowWorker;

/// <summary>
/// Custom AssemblyLoadContext for loading plugins with proper dependency isolation.
/// Uses AssemblyDependencyResolver to resolve dependencies from the plugin's .deps.json file.
///
/// This provides several benefits over Assembly.LoadFrom():
/// - Dependency isolation: Each plugin can have its own version of dependencies
/// - .deps.json support: Automatic resolution of transitive dependencies
/// - Native library support: Proper resolution of unmanaged DLLs
/// - Type identity safety: Shared types (from Contracts) are loaded once from default context
/// </summary>
internal sealed class PluginLoadContext : AssemblyLoadContext
{
    private readonly AssemblyDependencyResolver _resolver;

    /// <summary>
    /// Creates a new PluginLoadContext for loading a plugin assembly.
    /// </summary>
    /// <param name="pluginPath">The full path to the plugin assembly (.dll file).</param>
    public PluginLoadContext(string pluginPath) : base(isCollectible: false)
    {
        _resolver = new AssemblyDependencyResolver(pluginPath);
    }

    /// <summary>
    /// Attempts to load an assembly from the plugin's dependencies.
    /// </summary>
    /// <param name="assemblyName">The name of the assembly to load.</param>
    /// <returns>The loaded assembly, or null to fall back to the default context.</returns>
    protected override Assembly? Load(AssemblyName assemblyName)
    {
        // Try to resolve from plugin's dependencies using .deps.json
        string? assemblyPath = _resolver.ResolveAssemblyToPath(assemblyName);
        if (assemblyPath != null)
        {
            return LoadFromAssemblyPath(assemblyPath);
        }

        // Return null to fall back to default context.
        // This ensures shared types (ISharedState, ITaskHandler, etc. from Contracts)
        // are loaded from the host's context, maintaining type identity.
        return null;
    }

    /// <summary>
    /// Attempts to load an unmanaged (native) library from the plugin's dependencies.
    /// </summary>
    /// <param name="unmanagedDllName">The name of the unmanaged library to load.</param>
    /// <returns>A handle to the loaded library, or IntPtr.Zero to use default resolution.</returns>
    protected override IntPtr LoadUnmanagedDll(string unmanagedDllName)
    {
        string? libraryPath = _resolver.ResolveUnmanagedDllToPath(unmanagedDllName);
        if (libraryPath != null)
        {
            return LoadUnmanagedDllFromPath(libraryPath);
        }
        return IntPtr.Zero;
    }
}
