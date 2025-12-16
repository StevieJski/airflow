// Compatibility shims for older target frameworks (netstandard2.0)
// Provides minimal definitions for compiler-required types such as
// IsExternalInit and a few attributes so newer C# language features
// (init-only, required) can be used while targeting older frameworks.

#if NETSTANDARD2_0
using System;

namespace System.Runtime.CompilerServices
{
    // Marker type used by C# for init-only setters.
    public static class IsExternalInit { }

    // Minimal RequiredMemberAttribute used by the compiler.
    [AttributeUsage(AttributeTargets.All, Inherited = false)]
    public sealed class RequiredMemberAttribute : Attribute
    {
        public RequiredMemberAttribute() { }
    }

    // Minimal CompilerFeatureRequiredAttribute used by the compiler.
    [AttributeUsage(AttributeTargets.All, AllowMultiple = true, Inherited = false)]
    public sealed class CompilerFeatureRequiredAttribute : Attribute
    {
        public CompilerFeatureRequiredAttribute(string feature) { }
    }
}

namespace System.Diagnostics.CodeAnalysis
{
    // Minimal SetsRequiredMembersAttribute used by the compiler.
    [AttributeUsage(AttributeTargets.Constructor | AttributeTargets.Method | AttributeTargets.Property | AttributeTargets.Field | AttributeTargets.Class | AttributeTargets.Struct, Inherited = false)]
    public sealed class SetsRequiredMembersAttribute : Attribute
    {
        public SetsRequiredMembersAttribute() { }
    }
}

#endif
