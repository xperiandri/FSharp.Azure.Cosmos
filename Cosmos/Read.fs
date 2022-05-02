[<AutoOpen>]
module FSharp.Azure.Cosmos.Read

open Microsoft.Azure.Cosmos

[<Struct>]
type ReadOperation<'a> =
    { Id : string
      PartitionKey : PartitionKey
      RequestOptions : ItemRequestOptions voption }

open System

type ReadBuilder<'a>() =
    member _.Yield _ =
        {
            Id = String.Empty
            PartitionKey = PartitionKey.None
            RequestOptions = ValueNone
        } : ReadOperation<'a>

    /// Sets the item being creeated
    [<CustomOperation "id">]
    member _.Id (state : inref<ReadOperation<_>>, id) = { state with Id = id }

    /// Sets the partition key
    [<CustomOperation "partitionKey">]
    member _.PartitionKey (state : inref<ReadOperation<_>>, partitionKey: PartitionKey) = { state with PartitionKey = partitionKey }

    /// Sets the partition key
    [<CustomOperation "partitionKeyValue">]
    member _.PartitionKeyValue (state : inref<ReadOperation<_>>, partitionKey: string) = { state with PartitionKey = PartitionKey partitionKey }

    /// Sets the request options
    [<CustomOperation "requestOptions">]
    member _.RequestOptions (state : inref<ReadOperation<_>>, options: ItemRequestOptions) = { state with RequestOptions = ValueSome options }

    /// Sets the eTag to <see href="IfNotMatchEtag">IfNotMatchEtag</see>
    [<CustomOperation "eTagValue">]
    member _.ETagValue (state : UpsertOperation<_>, eTag: string) =
        match state.RequestOptions with
        | ValueSome options ->
            options.IfNotMatchEtag <- eTag
            state
        | ValueNone ->
            let options = ItemRequestOptions (IfNotMatchEtag = eTag)
            { state with RequestOptions = ValueSome options }

let read<'a> = ReadBuilder<'a>()

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

type Microsoft.Azure.Cosmos.Container with

    member container.AsyncExecute(operation : ReadOperation<'a>, success, failure) = async {
        if String.IsNullOrEmpty operation.Id then invalidArg "id" "Read operation requires Id"
        if operation.PartitionKey = Unchecked.defaultof<PartitionKey> then invalidArg "partitionKey" "Read operation requires PartitionKey"

        let! ct = Async.CancellationToken
        try
            let! result = container.ReadItemAsync<'a>(operation.Id,
                                                      operation.PartitionKey,
                                                      operation.RequestOptions |> ValueOption.toObj,
                                                      cancellationToken = ct)
            return CosmosResponse.fromItemResponse (success) result
        with
        | HandleException ex -> return CosmosResponse.fromException (failure) ex
    }

    member container.AsyncExecute (operation : ReadOperation<'a>) =
        let successFn =
            function
            | null -> ReadResult.NotChanged
            | item -> ReadResult.Ok item
        container.AsyncExecute (operation, successFn, toReadResult ReadResult.NotFound)

    member container.AsyncExecuteOption (operation : ReadOperation<'a>) =
        container.AsyncExecute (operation, Some, toReadResult (fun _ -> None))

    member container.AsyncExecuteValueOption (operation : ReadOperation<'a>) =
        container.AsyncExecute (operation, ValueSome, toReadResult (fun _ -> ValueNone))

