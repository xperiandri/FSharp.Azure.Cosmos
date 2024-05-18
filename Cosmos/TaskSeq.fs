module Microsoft.Azure.Cosmos.TaskSeq

open System.Linq
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open System.Threading
open Microsoft.Azure.Cosmos
open Microsoft.Azure.Cosmos.Linq
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

/// <summary>
/// Asynchronously iterates Cosmos DB <see cref="FeedIterator{T}" />.
/// </summary>
/// <param name="iterator">Cosmos DB feed iterator</param>
let ofFeedIterator<'T> (iterator : FeedIterator<'T>) = iterator.AsAsyncEnumerable<'T> ()

/// <summary>
/// Asynchronously iterates Cosmos DB <see cref="FeedIterator{T}" />.
/// </summary>
/// <param name="cancellationToken">Cancellation token</param>
/// <param name="iterator">Cosmos DB feed iterator</param>
let ofFeedIteratorWithCancellation<'T> (cancellationToken : CancellationToken) (iterator : FeedIterator<'T>) =
    iterator.AsAsyncEnumerable<'T> (cancellationToken)

/// <summary>
/// Asynchronously iterates Cosmos DB <see cref="FeedIterator{T}" /> producing items with their ETag values.
/// </summary>
/// <param name="iterator">Cosmos DB feed iterator</param>
let ofFeedIteratorWithETags<'T> (iterator : FeedIterator<'T>) = iterator.AsAsyncEnumerableWithTags<'T> ()

/// <summary>
/// Asynchronously iterates Cosmos DB <see cref="FeedIterator{T}" /> producing items with their ETag values.
/// </summary>
/// <param name="cancellationToken">Cancellation token</param>
/// <param name="iterator">Cosmos DB feed iterator</param>
let ofFeedIteratorWithETagsAndCancellation<'T> (cancellationToken : CancellationToken) (iterator : FeedIterator<'T>) =
    iterator.AsAsyncEnumerableWithTags<'T> (cancellationToken)

/// <summary>
/// Created Cosmos DB <see cref="FeedIterator{T}" /> from <see cref="IQueryable{T}" /> and asynchronously iterates it.
/// </summary>
/// <param name="query">Cosmos DB querable</param>
let ofCosmosDbQuerable<'T> (query : IQueryable<'T>) = query.ToFeedIterator().AsAsyncEnumerable<'T> ()

/// <summary>
/// Created Cosmos DB <see cref="FeedIterator{T}" /> from <see cref="IQueryable{T}" /> and asynchronously iterates it.
/// </summary>
/// <param name="query">Cosmos DB querable</param>
/// <param name="cancellationToken">Cancellation token</param>
let ofCosmosDbQuerableWithCancellation<'T> (cancellationToken : CancellationToken) (query : IQueryable<'T>) = query.ToFeedIterator().AsAsyncEnumerable<'T> (cancellationToken)
