[<AutoOpen>]
module FSharp.Azure.Cosmos.ReadMany

open System
open System.Collections.Immutable
open Microsoft.Azure.Cosmos

[<Struct>]
type ReadManyOperation<'T> = {
    Items : ValueTuple<string, PartitionKey> ImmutableArray
    RequestOptions : ReadManyRequestOptions
}

type ReadManyBuilder<'T> () =
    member _.Yield _ =
        {
            Items = ImmutableArray.Empty
            RequestOptions = Unchecked.defaultof<_>
        }
        : ReadManyOperation<'T>

    /// Sets the item being created
    [<CustomOperation "item">]
    member _.Item (state : ReadManyOperation<_>, id, partitionKey : PartitionKey) =
        let items = state.Items.Add (id, partitionKey)
        { state with Items = items }

    /// Sets the item being created
    [<CustomOperation "item">]
    member builder.Item (state : ReadManyOperation<_>, id, partitionKey : string) =
        builder.Item (state, id, PartitionKey partitionKey)

    /// Sets the items being created
    [<CustomOperation "items">]
    member _.Items (state : ReadManyOperation<_>, items : ValueTuple<string, PartitionKey> seq) =
        let items = state.Items.AddRange items
        { state with Items = items }

    /// Sets the items being created
    [<CustomOperation "items">]
    member builder.Items (state : ReadManyOperation<_>, items : ValueTuple<string, string> seq) =
        builder.Items (
            state,
            items
            |> Seq.map (fun struct (id, partitionKey) -> struct (id, PartitionKey partitionKey))
        )

    /// Sets the request options
    [<CustomOperation "requestOptions">]
    member _.RequestOptions (state : ReadManyOperation<_>, options : ReadManyRequestOptions) = {
        state with
            RequestOptions = options
    }

    /// <summary>Sets the eTag to <see cref="ReadManyRequestOptions.IfNotMatchEtag"/></summary>
    [<CustomOperation "eTag">]
    member _.ETag (state : ReadManyOperation<_>, eTag : string) =
        if state.RequestOptions = Unchecked.defaultof<_> then
            let options = ReadManyRequestOptions (IfNoneMatchEtag = eTag)
            { state with RequestOptions = options }
        else
            state.RequestOptions.IfNoneMatchEtag <- eTag
            state

    // ------------------------------------------- Request options -------------------------------------------
    /// <summary>Sets the operation <see cref="ConsistencyLevel"/></summary>
    [<CustomOperation "consistencyLevel">]
    member _.ConsistencyLevel (state : ReadManyOperation<_>, consistencyLevel : ConsistencyLevel Nullable) =
        state.RequestOptions.ConsistencyLevel <- consistencyLevel
        state

    /// Sets the session token
    [<CustomOperation "sessionToken">]
    member _.SessionToken (state : ReadManyOperation<_>, sessionToken : string) =
        state.RequestOptions.SessionToken <- sessionToken
        state

let readMany<'T> = ReadManyBuilder<'T> ()

// https://docs.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb

/// Represents the result of a read operation.
type ReadManyResult<'t> =
    | Ok of 't // 200
    | NotModified // 204
    | IncompatibleConsistencyLevel of ResponseBody : string // 400
    | NotFound of ResponseBody : string // 404

open System.Net

let private toReadResult badRequestCtor notFoundResultCtor (ex : CosmosException) =
    match ex.StatusCode with
    | HttpStatusCode.BadRequest -> badRequestCtor ex.ResponseBody
    | HttpStatusCode.NotFound -> notFoundResultCtor ex.ResponseBody
    | _ -> raise ex

open System.Runtime.InteropServices
open System.Threading
open System.Threading.Tasks

type Microsoft.Azure.Cosmos.Container with

    /// <summary>
    /// Executes a read many operation and returns <see cref="ItemResponse{T}"/>.
    /// </summary>
    /// <param name="operation">Read many operation</param>
    /// <param name="cancellationToken">Cancellation token</param>
    member container.PlainExecuteAsync<'T>
        (operation : ReadManyOperation<'T>, [<Optional>] cancellationToken : CancellationToken)
        =
        container.ReadManyItemsAsync<'T> (operation.Items, operation.RequestOptions, cancellationToken = cancellationToken)

    /// <summary>
    /// Executes a read many operation, transforms success or failure, and returns <see cref="CosmosResponse{T}"/>.
    /// </summary>
    /// <param name="operation">Read operation</param>
    /// <param name="success">Result transform if success</param>
    /// <param name="failure">Error transform if failure</param>
    /// <param name="cancellationToken">Cancellation token</param>
    member container.ExecuteAsync<'T, 'Result>
        (operation : ReadManyOperation<'T>, success, failure, [<Optional>] cancellationToken : CancellationToken)
        : Task<CosmosResponse<'Result>>
        =
        task {
            try
                let! result = container.PlainExecuteAsync (operation, cancellationToken)
                return CosmosResponse.fromFeedResponse (success) result
            with HandleException ex ->
                return CosmosResponse.fromException (failure) ex
        }

    /// <summary>
    /// Executes a read many operation and returns <see cref="CosmosResponse{ReadResult{T}}"/>.
    /// </summary>
    /// <param name="operation">Read operation</param>
    /// <param name="cancellationToken">Cancellation token</param>
    member container.ExecuteAsync<'T> (operation : ReadManyOperation<'T>, [<Optional>] cancellationToken : CancellationToken) =
        let successFn result : ReadManyResult<FeedResponse<'T>> =
            if Object.Equals (result, Unchecked.defaultof<'T>) then
                ReadManyResult.NotModified
            else
                ReadManyResult.Ok result

        container.ExecuteAsync<'T, ReadManyResult<FeedResponse<'T>>> (
            operation,
            successFn,
            toReadResult ReadManyResult.IncompatibleConsistencyLevel ReadManyResult.NotFound,
            cancellationToken
        )
