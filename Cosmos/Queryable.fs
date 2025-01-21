[<AutoOpen>]
module Microsoft.Azure.Cosmos.QuerableExtensions

open System.Linq
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open System.Threading
open Microsoft.Azure.Cosmos
open Microsoft.Azure.Cosmos.Linq

type IQueryable<'T> with

    member inline query.AsAsyncEnumerable<'T> ([<Optional; EnumeratorCancellation>] cancellationToken : CancellationToken) =
        query.ToFeedIterator().AsAsyncEnumerable<'T> (cancellationToken)

    member inline query.MapAsyncEnumerable<'T, 'Result> (mapping : IterationState<'T> -> 'T -> 'Result, [<Optional; EnumeratorCancellation>] cancellationToken : CancellationToken) =
        query.ToFeedIterator().MapAsyncEnumerable<'T, 'Result> (mapping, cancellationToken)

    member inline query.MapFoldAsyncEnumerable<'T, 'State, 'Result> (mapping : IterationState<'T> -> 'State -> 'T -> struct ('Result * 'State), state : 'State, [<Optional; EnumeratorCancellation>] cancellationToken : CancellationToken) =
        query.ToFeedIterator().MapFoldAsyncEnumerable<'T, 'State, 'Result> (mapping, state, cancellationToken)
