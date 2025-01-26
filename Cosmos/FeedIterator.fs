[<AutoOpen>]
module Microsoft.Azure.Cosmos.FeedIteratorExtensions

open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open System.Threading
open Microsoft.Azure.Cosmos
open FSharp.Control

[<Struct>]
type IterationState<'T> = {
    /// The total number of items read.
    ItemsRead : int
    /// The number of items read in the current page.
    i : int
    /// The continuation token of the current item.
    ContinuationToken : string | null
    /// The current page.
    Page : FeedResponse<'T>
    /// A flag to stop the iteration.
    StopRef : bool Ref
} with

    member this.ShouldStop
        with get () = this.StopRef.Value
        and set v = this.StopRef.Value <- v
    member this.Stop () = this.StopRef.Value <- true

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

    /// <summary>Converts the iterator to an async sequence of items mapping each item.</summary>
    /// <param name="mapping">A function to transform items from the input sequence.</param>
    member iterator.MapAsyncEnumerable<'T, 'Result>
        (map : IterationState<'T> -> 'T -> 'Result, [<Optional; EnumeratorCancellation>] cancellationToken : CancellationToken)
        =
        taskSeq {
            let mutable iterationState = {
                ItemsRead = 0
                i = 0
                ContinuationToken = null
                Page = Unchecked.defaultof<_>
                StopRef = ref false
            }
            while iterator.HasMoreResults && not iterationState.ShouldStop do
                cancellationToken.ThrowIfCancellationRequested ()
                let! page = iterator.ReadNextAsync (cancellationToken)
                iterationState <- { iterationState with i = 0; Page = page }

                yield!
                    page
                    |> Seq.mapi (fun i item ->
                        let continuationToken =
                            if i > page.Count - 2 then
                                page.ContinuationToken
                            else
                                iterationState.ContinuationToken
                        iterationState <- {
                            iterationState with
                                ItemsRead = iterationState.ItemsRead + 1
                                i = i + 1
                                ContinuationToken = continuationToken
                        }
                        map iterationState item
                    )

        }

    /// <summary>Combines map and fold. Converts the iterator to an async sequence of items mapping each item with intermediate state.</summary>
    /// <param name="mapping">The function to transform elements from the input collection and accumulate intermidiate value.</param>
    /// <param name="state">The initial intermediate state.</param>
    member iterator.MapFoldAsyncEnumerable<'T, 'State, 'Result>
        (
            map : IterationState<'T> -> 'State -> 'T -> struct ('Result * 'State),
            state : 'State,
            [<Optional; EnumeratorCancellation>] cancellationToken : CancellationToken
        )
        =
        let mutable state = state
        taskSeq {
            let mutable iterationState = {
                ItemsRead = 0
                i = 0
                ContinuationToken = null
                Page = Unchecked.defaultof<_>
                StopRef = ref false
            }
            while iterator.HasMoreResults && not iterationState.ShouldStop do
                cancellationToken.ThrowIfCancellationRequested ()
                let! page = iterator.ReadNextAsync (cancellationToken)
                iterationState <- { iterationState with i = 0; Page = page }

                yield!
                    page
                    |> Seq.mapi (fun i item ->
                        let continuationToken =
                            if i > page.Count - 2 then
                                page.ContinuationToken
                            else
                                iterationState.ContinuationToken
                        iterationState <- {
                            iterationState with
                                ItemsRead = iterationState.ItemsRead + 1
                                i = i + 1
                                ContinuationToken = continuationToken
                        }
                        let struct (item, currentState) = map iterationState state item
                        state <- currentState
                        item
                    )
        }
