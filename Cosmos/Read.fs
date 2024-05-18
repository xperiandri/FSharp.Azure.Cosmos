[<AutoOpen>]
module FSharp.Azure.Cosmos.Read

open Microsoft.Azure.Cosmos

[<Struct>]
type ReadOperation<'T> = {
    Id : string
    PartitionKey : PartitionKey
    RequestOptions : ItemRequestOptions
}

open System

type ReadBuilder<'T> () =
    member _.Yield _ =
        {
            Id = String.Empty
            PartitionKey = PartitionKey.None
            RequestOptions = Unchecked.defaultof<_>
        }
        : ReadOperation<'T>

    /// Sets the item being creeated
    [<CustomOperation "id">]
    member _.Id (state : ReadOperation<_>, id) = { state with Id = id }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member _.PartitionKey (state : ReadOperation<_>, partitionKey : PartitionKey) = {
        state with
            PartitionKey = partitionKey
    }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member _.PartitionKey (state : ReadOperation<_>, partitionKey : string) = {
        state with
            PartitionKey = PartitionKey partitionKey
    }

    /// Sets the request options
    [<CustomOperation "requestOptions">]
    member _.RequestOptions (state : ReadOperation<_>, options : ItemRequestOptions) = {
        state with
            RequestOptions = options
    }

    /// Sets the eTag to <see href="IfNotMatchEtag">IfNotMatchEtag</see>
    [<CustomOperation "eTag">]
    member _.ETag (state : UpsertOperation<_>, eTag : string) =
        if state.RequestOptions = Unchecked.defaultof<_> then
            let options = ItemRequestOptions (IfNoneMatchEtag = eTag)

            { state with RequestOptions = options }
        else
            state.RequestOptions.IfNoneMatchEtag <- eTag
            state

let read<'T> = ReadBuilder<'T> ()

// https://docs.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb

/// Represents the result of a read operation.
type ReadResult<'t> =
    | Ok of 't // 200
    | NotChanged // 204
    | NotFound of ResponseBody : string // 404

open System.Net

let private toReadResult notFoundResultCtor (ex : CosmosException) =
    match ex.StatusCode with
    | HttpStatusCode.NotFound -> notFoundResultCtor ex.ResponseBody
    | _ -> raise ex

open System.Runtime.InteropServices
open System.Threading
open System.Threading.Tasks

type Microsoft.Azure.Cosmos.Container with

    /// <summary>
    /// Executes a read operation and returns <see cref="ItemResponse{T}"/>.
    /// </summary>
    /// <param name="operation">Read operation</param>
    /// <param name="cancellationToken">Cancellation token</param>
    member container.PlainExecuteAsync (operation : ReadOperation<'T>, [<Optional>] cancellationToken : CancellationToken) =
        container.ReadItemAsync<'T> (
            operation.Id,
            operation.PartitionKey,
            operation.RequestOptions,
            cancellationToken = cancellationToken
        )

    /// <summary>
    /// Executes a read operation, transforms success or failure, and returns <see cref="CosmosResponse{T}"/>.
    /// </summary>
    /// <param name="operation">Read operation</param>
    /// <param name="success">Result transform if success</param>
    /// <param name="failure">Error transform if failure</param>
    /// <param name="cancellationToken">Cancellation token</param>
    member container.ExecuteAsync
        (operation : ReadOperation<'T>, success, failure, [<Optional>] cancellationToken : CancellationToken)
        =
        task {
            try
                let! result = container.PlainExecuteAsync (operation, cancellationToken)
                return CosmosResponse.fromItemResponse (success) result
            with HandleException ex ->
                return CosmosResponse.fromException (failure) ex
        }

    /// <summary>
    /// Executes a read operation and returns <see cref="CosmosResponse{ReadResult{T}}"/>.
    /// </summary>
    /// <param name="operation">Read operation</param>
    /// <param name="cancellationToken">Cancellation token</param>
    member container.ExecuteAsync (operation : ReadOperation<'T>, [<Optional>] cancellationToken : CancellationToken) =
        let successFn =
            function
            | null -> ReadResult.NotChanged
            | item -> ReadResult.Ok item

        container.ExecuteAsync (operation, successFn, toReadResult ReadResult.NotFound, cancellationToken)

    /// <summary>
    /// Executes a read operation and returns <see cref="CosmosResponse{FSharpValueOption{T}}"/>.
    /// </summary>
    /// <param name="operation">Read operation</param>
    /// <param name="cancellationToken">Cancellation token</param>
    member container.ExecuteAsyncOption (operation : ReadOperation<'T>, [<Optional>] cancellationToken : CancellationToken) =
        container.ExecuteAsync (operation, Some, toReadResult (fun _ -> None), cancellationToken)

    /// <summary>
    /// Executes a read operation and returns <see cref="CosmosResponse{FSharpOption{T}}"/>.
    /// </summary>
    /// <param name="operation">Read operation</param>
    /// <param name="cancellationToken">Cancellation token</param>
    member container.ExecuteAsyncValueOption (operation : ReadOperation<'T>, [<Optional>] cancellationToken : CancellationToken) =
        container.ExecuteAsync (operation, ValueSome, toReadResult (fun _ -> ValueNone), cancellationToken)
