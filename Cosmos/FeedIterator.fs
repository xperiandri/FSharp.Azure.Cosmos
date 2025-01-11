[<AutoOpen>]
module Microsoft.Azure.Cosmos.FeedIteratorExtensions

open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open System.Threading
open Microsoft.Azure.Cosmos
open FSharp.Control

// See https://github.com/Azure/azure-cosmos-dotnet-v3/issues/903
type FeedIterator<'T> with

    /// Converts the iterator to an async sequence of items.
    member iterator.AsAsyncEnumerable<'T> ([<Optional; EnumeratorCancellation>] cancellationToken : CancellationToken) = taskSeq {
        while iterator.HasMoreResults do
            let! page = iterator.ReadNextAsync (cancellationToken)

            for item in page do
                cancellationToken.ThrowIfCancellationRequested ()
                yield item
    }

    /// Converts the iterator to an async sequence of items with continuation token.
    member iterator.AsContinuableAsyncEnumerable<'T> ([<Optional; EnumeratorCancellation>] cancellationToken : CancellationToken) = taskSeq {
        while iterator.HasMoreResults do
            let! page = iterator.ReadNextAsync (cancellationToken)

            for item in page do
                cancellationToken.ThrowIfCancellationRequested ()
                yield struct (item, page.ContinuationToken)
    }

    /// Converts the iterator to an async sequence of items with continuation token.
    member iterator.AsContinuableAsyncEnumerable<'T> (desiredItemCount, [<Optional; EnumeratorCancellation>] cancellationToken : CancellationToken) = taskSeq {
        let mutable itemsRead = 0
        while iterator.HasMoreResults && itemsRead < desiredItemCount do
            let! page = iterator.ReadNextAsync (cancellationToken)

            for item in page do
                cancellationToken.ThrowIfCancellationRequested ()
                itemsRead <- itemsRead + 1
                yield struct (item, page.ContinuationToken)
    }

    /// Converts the iterator to an async sequence of items with their ETag values.
    member iterator.AsTaggedAsyncEnumerable<'T>  ([<Optional; EnumeratorCancellation>] cancellationToken : CancellationToken) = taskSeq {
        while iterator.HasMoreResults do
            let! page = iterator.ReadNextAsync (cancellationToken)

            for item in page do
                cancellationToken.ThrowIfCancellationRequested ()
                yield struct (item, page.ETag)
    }

