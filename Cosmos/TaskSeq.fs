module Microsoft.Azure.Cosmos.TaskSeq

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

    /// Converts the iterator to an async sequence of items with their ETag values.
    member iterator.AsAsyncEnumerableWithTags<'T> ([<Optional; EnumeratorCancellation>] cancellationToken : CancellationToken) = taskSeq {
        while iterator.HasMoreResults do
            let! page = iterator.ReadNextAsync (cancellationToken)

            for item in page do
                cancellationToken.ThrowIfCancellationRequested ()
                yield struct (item, page.ETag)
    }

let ofFeedIterator<'T> (iterator : FeedIterator<'T>) = iterator.AsAsyncEnumerable<'T> ()

let ofFeedIteratorWithCancellation<'T> (cancellationToken : CancellationToken) (iterator : FeedIterator<'T>) =
    iterator.AsAsyncEnumerable<'T> (cancellationToken)

let ofFeedIteratorWithTags<'T> (iterator : FeedIterator<'T>) = iterator.AsAsyncEnumerableWithTags<'T> ()

let ofFeedIteratorWithTagsAndCancellation<'T> (cancellationToken : CancellationToken) (iterator : FeedIterator<'T>) =
    iterator.AsAsyncEnumerableWithTags<'T> (cancellationToken)
