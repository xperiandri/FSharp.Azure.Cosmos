namespace Microsoft.Azure.Cosmos

open System.Linq
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open System.Threading
open Microsoft.Azure.Cosmos
open Microsoft.Azure.Cosmos.Linq

[<AbstractClass; Sealed; Extension>]
type QuerableExtensions private () =

    [<Extension; CompiledName "AsAsyncEnumerable">]
    static member inline AsAsyncEnumerable<'T> (query : IQueryable<'T>, [<Optional; EnumeratorCancellation>] cancellationToken : CancellationToken) =
        query.ToFeedIterator().AsAsyncEnumerable<'T> (cancellationToken)

    [<Extension; CompiledName "AsContinuableAsyncEnumerable">]
    static member inline AsContinuableAsyncEnumerable<'T> (query : IQueryable<'T>, [<Optional; EnumeratorCancellation>] cancellationToken : CancellationToken) =
        query.ToFeedIterator().AsContinuableAsyncEnumerable<'T> (cancellationToken)

    [<Extension; CompiledName "AsContinuableAsyncEnumerable">]
    static member inline AsContinuableAsyncEnumerable<'T> (query : IQueryable<'T>, desiredItemCount, [<Optional; EnumeratorCancellation>] cancellationToken : CancellationToken) =
        query.ToFeedIterator().AsContinuableAsyncEnumerable<'T> (desiredItemCount, cancellationToken)
