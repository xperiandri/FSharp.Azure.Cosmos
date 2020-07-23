module Microsoft.Azure.Cosmos.AsyncSeq

open System.Runtime.CompilerServices
open System.Threading
open FSharp.Control
open Microsoft.Azure.Cosmos

// See https://github.com/Azure/azure-cosmos-dotnet-v3/issues/903
type FeedIterator<'T> with

    member iterator.AsAsyncEnumerable<'T>
        ([<EnumeratorCancellation>] cancellationToken : CancellationToken) = asyncSeq {
        while iterator.HasMoreResults do
            let! page = iterator.ReadNextAsync(cancellationToken) |> Async.AwaitTask
            for item in page do
                cancellationToken.ThrowIfCancellationRequested()
                yield item
    }

    member iterator.AsAsyncEnumerable<'T> () = asyncSeq {
        let! ct = Async.CancellationToken
        while iterator.HasMoreResults do
            let! page = iterator.ReadNextAsync(ct) |> Async.AwaitTask
            for item in page do
                ct.ThrowIfCancellationRequested()
                yield item
    }

    member iterator.AsAsyncEnumerableWithTags<'T>
        ([<EnumeratorCancellation>] cancellationToken : CancellationToken) = asyncSeq {
        while iterator.HasMoreResults do
            let! page = iterator.ReadNextAsync(cancellationToken) |> Async.AwaitTask
            for item in page do
                cancellationToken.ThrowIfCancellationRequested()
                yield struct (item, page.ETag)
    }

    member iterator.AsAsyncEnumerableWithTags<'T> () = asyncSeq {
        let! ct = Async.CancellationToken
        while iterator.HasMoreResults do
            let! page = iterator.ReadNextAsync(ct) |> Async.AwaitTask
            for item in page do
                ct.ThrowIfCancellationRequested()
                yield struct (item, page.ETag)
    }


let ofFeedIterator<'T> (iterator : FeedIterator<'T>) = iterator.AsAsyncEnumerable<'T>()

let ofFeedIteratorWithTags<'T> (iterator : FeedIterator<'T>) = iterator.AsAsyncEnumerableWithTags<'T>()
