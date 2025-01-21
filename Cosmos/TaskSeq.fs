module Microsoft.Azure.Cosmos.TaskSeq

open System.Linq
open System.Threading
open Microsoft.Azure.Cosmos
open Microsoft.Azure.Cosmos.Linq

/// <summary>
/// Executes Cosmos DB query and asynchronously iterates Cosmos DB <see cref="FeedIterator{T}" />.
/// </summary>
/// <param name="iterator">Cosmos DB feed iterator</param>
let ofFeedIterator<'T> (iterator : FeedIterator<'T>) = iterator.AsAsyncEnumerable<'T> ()

/// <summary>
/// Executes Cosmos DB query and asynchronously iterates Cosmos DB <see cref="FeedIterator{T}" />
/// mapping each item.
/// </summary>
/// <param name="mapping">A function to transform items from the input sequence.</param>
/// <param name="iterator">Cosmos DB feed iterator</param>
let mapOfFeedIterator<'T, 'Result> (mapping : IterationState<'T> -> 'T -> 'Result) (iterator : FeedIterator<'T>) =
    iterator.MapAsyncEnumerable<'T, 'Result> (mapping)

/// <summary>
/// Executes Cosmos DB query and asynchronously iterates Cosmos DB <see cref="FeedIterator{T}" />
/// mapping each item and accumulating intermidiate value.
/// </summary>
/// <param name="mapping">
/// The function to transform elements from the input collection and accumulate intermidiate value.
/// </param>
/// <param name="state">The initial intermediate state.</param>
let mapFoldOfFeedIterator<'T, 'State, 'Result>
    (mapping : IterationState<'T> -> 'State -> 'T -> struct ('Result * 'State))
    (state : 'State)
    (iterator : FeedIterator<'T>)
    =
    iterator.MapFoldAsyncEnumerable<'T, 'State, 'Result> (mapping, state)

/// <summary>
/// Executes Cosmos DB query and asynchronously iterates Cosmos DB <see cref="FeedIterator{T}" />.
/// </summary>
/// <param name="cancellationToken">Cancellation token</param>
/// <param name="iterator">Cosmos DB feed iterator</param>
let ofFeedIteratorWithCancellation<'T> (cancellationToken : CancellationToken) (iterator : FeedIterator<'T>) =
    iterator.AsAsyncEnumerable<'T> (cancellationToken)

/// <summary>
/// Executes Cosmos DB query and asynchronously iterates Cosmos DB <see cref="FeedIterator{T}" />
/// mapping each item.
/// </summary>
/// <param name="cancellationToken">Cancellation token</param>
/// <param name="mapping">A function to transform items from the input sequence.</param>
/// <param name="iterator">Cosmos DB feed iterator</param>
let mapOfFeedIteratorWithCancellation<'T, 'Result>
    (cancellationToken : CancellationToken)
    (mapping : IterationState<'T> -> 'T -> 'Result)
    (iterator : FeedIterator<'T>)
    =
    iterator.MapAsyncEnumerable<'T, 'Result> (mapping, cancellationToken)

/// <summary>
/// Executes Cosmos DB query and asynchronously iterates Cosmos DB <see cref="FeedIterator{T}" />
/// mapping each item and accumulating intermidiate value.
/// </summary>
/// <param name="mapping">
/// The function to transform elements from the input collection and accumulate intermidiate value.
/// </param>
/// <param name="state">The initial intermediate state.</param>
let mapFoldOfFeedIteratorWithCancellation<'T, 'State, 'Result>
    (cancellationToken : CancellationToken)
    (mapping : IterationState<'T> -> 'State -> 'T -> struct ('Result * 'State))
    (state : 'State)
    (iterator : FeedIterator<'T>)
    =
    iterator.MapFoldAsyncEnumerable<'T, 'State, 'Result> (mapping, state, cancellationToken)

/// <summary>
/// Created Cosmos DB <see cref="FeedIterator{T}" /> from <see cref="IQueryable{T}" />
/// and asynchronously iterates it.
/// </summary>
/// <param name="query">Cosmos DB querable</param>
let ofCosmosDbQuerable<'T> (query : IQueryable<'T>) = query.ToFeedIterator().AsAsyncEnumerable<'T> ()

/// <summary>
/// Created Cosmos DB <see cref="FeedIterator{T}" /> from <see cref="IQueryable{T}" />
/// and asynchronously iterates it mapping each item.
/// </summary>
/// <param name="mapping">A function to transform items from the input sequence.</param>
let mapOfCosmosDbQuerable<'T, 'Result> (mapping : IterationState<'T> -> 'T -> 'Result) (query : IQueryable<'T>) =
    query.ToFeedIterator().MapAsyncEnumerable<'T, 'Result> (mapping)

/// <summary>
/// Created Cosmos DB <see cref="FeedIterator{T}" /> from <see cref="IQueryable{T}" />
/// and asynchronously iterates it.
/// </summary>
/// <param name="query">Cosmos DB querable</param>
/// <param name="cancellationToken">Cancellation token</param>
let ofCosmosDbQuerableWithCancellation<'T> (cancellationToken : CancellationToken) (query : IQueryable<'T>) =
    query.ToFeedIterator().AsAsyncEnumerable<'T> (cancellationToken)

/// <summary>
/// Created Cosmos DB <see cref="FeedIterator{T}" /> from <see cref="IQueryable{T}" />
/// and asynchronously iterates it mapping each item.
/// </summary>
/// <param name="mapping">A function to transform items from the input sequence.</param>
/// <param name="query">Cosmos DB querable</param>
let mapOfCosmosDbQuerableWithCancellation<'T, 'Result>
    (cancellationToken : CancellationToken)
    (mapping : IterationState<'T> -> 'T -> 'Result)
    (query : IQueryable<'T>)
    =
    query.ToFeedIterator().MapAsyncEnumerable<'T, 'Result> (mapping, cancellationToken)

/// <summary>
/// Created Cosmos DB <see cref="FeedIterator{T}" /> from <see cref="IQueryable{T}" />
/// and asynchronously iterates it mapping each item and accumulating intermidiate value.
/// </summary>
/// <param name="mapping">The function to transform elements from the input collection and accumulate intermidiate value.</param>
/// <param name="state">The initial intermediate state.</param>
let mapFoldOfCosmosDbQuerableWithCancellation<'T, 'State, 'Result>
    (cancellationToken : CancellationToken)
    (mapping : IterationState<'T> -> 'State -> 'T -> struct ('Result * 'State))
    (state : 'State)
    (query : IQueryable<'T>)
    =
    query.ToFeedIterator().MapFoldAsyncEnumerable<'T, 'State, 'Result> (mapping, state, cancellationToken)
