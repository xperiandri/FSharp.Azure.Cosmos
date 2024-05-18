[<AutoOpen>]
module FSharp.Azure.Cosmos.Read

open Microsoft.Azure.Cosmos

[<Struct>]
type ReadOperation<'a> = {
    Id : string
    PartitionKey : PartitionKey
    RequestOptions : ItemRequestOptions
}

open System

type ReadBuilder<'a> () =
    member _.Yield _ =
        {
            Id = String.Empty
            PartitionKey = PartitionKey.None
            RequestOptions = Unchecked.defaultof<_>
        }
        : ReadOperation<'a>

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

let read<'a> = ReadBuilder<'a> ()

// https://docs.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb

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

    member container.ExecuteAsync
        (operation : ReadOperation<'a>, success, failure, [<Optional>] cancellationToken : CancellationToken)
        =
        task {
            try
                let! result =
                    container.ReadItemAsync<'a> (
                        operation.Id,
                        operation.PartitionKey,
                        operation.RequestOptions,
                        cancellationToken = cancellationToken
                    )

                return CosmosResponse.fromItemResponse (success) result
            with HandleException ex ->
                return CosmosResponse.fromException (failure) ex
        }

    member container.ExecuteAsync (operation : ReadOperation<'a>, [<Optional>] cancellationToken : CancellationToken) =
        let successFn =
            function
            | null -> ReadResult.NotChanged
            | item -> ReadResult.Ok item

        container.ExecuteAsync (operation, successFn, toReadResult ReadResult.NotFound, cancellationToken)

    member container.ExecuteAsyncOption (operation : ReadOperation<'a>, [<Optional>] cancellationToken : CancellationToken) =
        container.ExecuteAsync (operation, Some, toReadResult (fun _ -> None), cancellationToken)

    member container.ExecuteAsyncValueOption (operation : ReadOperation<'a>, [<Optional>] cancellationToken : CancellationToken) =
        container.ExecuteAsync (operation, ValueSome, toReadResult (fun _ -> ValueNone), cancellationToken)
